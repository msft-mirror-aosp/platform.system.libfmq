//! libfmq Rust wrapper

/*
* Copyright (C) 2024 The Android Open Source Project
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*      http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

use fmq_bindgen::{
    convertDesc, convertGrantor, descFlags, descGrantors, descHandleFDs, descHandleInts,
    descHandleNumFDs, descHandleNumInts, descNumGrantors, descQuantum, freeDesc,
    ndk_ScopedFileDescriptor, ErasedMessageQueue, ErasedMessageQueueDesc, GrantorDescriptor,
    MemTransaction, NativeHandle, ParcelFileDescriptor,
};
use log::error;
use zerocopy::TryFromBytes;

use std::marker::PhantomData;
use std::mem::MaybeUninit;
use std::ptr::{self, addr_of_mut, NonNull};
use std::sync::atomic::AtomicU32;
use std::time::Duration;

pub use fmq_bindgen::{MQDescriptor, SynchronizedReadWrite};

/// An IPC message queue for values of type T.
pub struct MessageQueue<T> {
    inner: ErasedMessageQueue,
    ty: PhantomData<T>,
}

/** A write completion from the MessageQueue::write() method.

This completion mutably borrows the MessageQueue to prevent concurrent writes;
these must be forbidden because the underlying AidlMessageQueue only stores the
number of outstanding writes, not which have and have not completed, so they
must complete in order. */
#[must_use]
pub struct WriteCompletion<'a, T: binder::WriteTo> {
    inner: MemTransaction,
    queue: &'a mut MessageQueue<T>,
    n_elems: usize,
    n_written: usize,
}

impl<T: binder::WriteTo> WriteCompletion<'_, T> {
    /// Obtain a pointer to the location at which the idx'th item should be
    /// stored.
    ///
    /// The returned pointer is correctly aligned for type `T`.
    ///
    /// The returned pointer is only valid while `self` has not been dropped and
    /// is invalidated by any call to `self.write`. The pointer should be used
    /// with `std::ptr::write_volatile` or a DMA API to initialize the
    /// underlying storage before calling `assume_written` to indicate how many
    /// elements were written.
    ///
    /// It is only permitted to access at most `contiguous_count(idx)` items
    /// via offsets from the returned address.
    ///
    /// Calling this method with a greater `idx` may return a pointer to another
    /// memory region of different size than the first.
    pub fn ptr(&self, idx: usize) -> *mut T {
        if idx >= self.n_elems {
            panic!(
                "indexing out of bound: ReadCompletion for {} elements but idx {} accessed",
                self.n_elems, idx
            )
        }
        ptr(&self.inner, idx)
    }

    /// Return the number of contiguous elements that may be stored starting at
    /// the given index in the backing buffer corresponding to the given index.
    ///
    /// Intended for use with the `ptr` method.
    ///
    /// Returns 0 if `idx` is greater than or equal to the completion's element
    /// count.
    pub fn contiguous_count(&self, idx: usize) -> usize {
        contiguous_count(&self.inner, idx, self.n_elems)
    }

    /// Returns how many elements still must be written to this WriteCompletion
    /// before dropping it.
    pub fn required_elements(&self) -> usize {
        assert!(self.n_written <= self.n_elems);
        self.n_elems - self.n_written
    }

    /// Write one item to `self`. Fails and returns the item if `self` is full.
    pub fn write(&mut self, data: T) -> Result<(), T> {
        if self.required_elements() > 0 {
            // SAFETY: `self.ptr(self.n_written)` returns a pointer aligned for
            // the type `T` into a shared-memory buffer that will live as long
            // as the `ErasedMessageQueue` that this `WriteCompletion` borrows.
            //
            // The dtor of data, if any, will not run because `data` is moved
            // out of here.
            unsafe { data.write_to_volatile(self.ptr(self.n_written)) };
            self.n_written += 1;
            Ok(())
        } else {
            Err(data)
        }
    }

    /// Promise to the `WriteCompletion` that `n_newly_written` elements have
    /// been written with unsafe code or DMA to the pointer returned by the
    /// `ptr` method.
    ///
    /// Panics if `n_newly_written` exceeds the number of elements yet required.
    ///
    /// # Safety
    /// It is UB to call this method except after calling the `ptr` method and
    /// writing the specified number of values of type T to that location.
    pub unsafe fn assume_written(&mut self, n_newly_written: usize) {
        assert!(n_newly_written <= self.required_elements());
        self.n_written += n_newly_written;
    }
}

impl<T: binder::WriteTo> Drop for WriteCompletion<'_, T> {
    fn drop(&mut self) {
        if self.n_written < self.n_elems {
            error!(
                "WriteCompletion dropped without writing to all elements ({}/{} written)",
                self.n_written, self.n_elems
            );
        }
        let txn = std::mem::take(&mut self.inner);
        self.queue.commit_write(txn);
    }
}

impl<T> MessageQueue<T> {
    const fn type_size() -> usize {
        std::mem::size_of::<T>()
    }

    /// Create a new MessageQueue with capacity for `elems` elements.
    ///
    /// If `event_word` is true, allocate an EventFlag word to use for
    /// synchronization on the status of the queue.
    pub fn new(elems: usize, event_word: bool) -> Self {
        Self {
            // SAFETY: Calling bindgen'd constructor. The only argument that
            // can't be validated by the implementation is the quantum, which
            // must equal the element size.
            inner: unsafe { ErasedMessageQueue::new1(elems, event_word, Self::type_size()) },
            ty: PhantomData,
        }
    }

    /// Create a MessageQueue connected to another existing instance from its
    /// descriptor.
    pub fn from_desc(desc: &MQDescriptor<T, SynchronizedReadWrite>, reset_pointers: bool) -> Self {
        let mut grantors = desc
            .grantors
            .iter()
            // SAFETY: this just forwards the integers to the GrantorDescriptor
            // constructor; GrantorDescriptor is POD.
            .map(|g| unsafe { convertGrantor(g.fdIndex, g.offset, g.extent) })
            .collect::<Vec<_>>();

        // SAFETY: These pointer/length pairs come from Vecs that will outlive
        // this function call, and the call itself copies all data it needs out
        // of them.
        let cpp_desc = unsafe {
            convertDesc(
                grantors.as_mut_ptr(),
                grantors.len(),
                desc.handle.fds.as_ptr().cast(),
                desc.handle.fds.len(),
                desc.handle.ints.as_ptr(),
                desc.handle.ints.len(),
                desc.quantum,
                desc.flags,
            )
        };
        // SAFETY: Calling bindgen'd constructor which does not store cpp_desc,
        // but just passes it to the initializer of AidlMQDescriptorShim, which
        // deep-copies it.
        let inner = unsafe { ErasedMessageQueue::new(cpp_desc, reset_pointers) };
        // SAFETY: we must free the desc returned by convertDesc; the pointer
        // was just returned above so we know it is valid.
        unsafe { freeDesc(cpp_desc) };
        Self { inner, ty: PhantomData }
    }

    /// Obtain a copy of the MessageQueue's descriptor, which may be used to
    /// access it remotely.
    pub fn dupe_desc(&self) -> MQDescriptor<T, SynchronizedReadWrite> {
        // SAFETY: dupeDesc may be called on any valid ErasedMessageQueue; it
        // simply forwards to dupeDesc on the inner AidlMessageQueue and wraps
        // in a heap allocation.
        let erased_desc: *mut ErasedMessageQueueDesc = unsafe { self.inner.dupeDesc() };
        let grantor_to_rust =
            |g: &fmq_bindgen::aidl_android_hardware_common_fmq_GrantorDescriptor| {
                GrantorDescriptor { fdIndex: g.fdIndex, offset: g.offset, extent: g.extent }
            };

        let scoped_to_parcel_fd = |fd: &ndk_ScopedFileDescriptor| {
            use std::os::fd::{BorrowedFd, FromRawFd, OwnedFd};
            // SAFETY: the fd is already open as an invariant of ndk::ScopedFileDescriptor, so
            // it will not be -1, as required by BorrowedFd.
            let borrowed = unsafe { BorrowedFd::borrow_raw(fd._base as i32) };
            ParcelFileDescriptor::new(match borrowed.try_clone_to_owned() {
                Ok(fd) => fd,
                Err(e) => {
                    error!("could not dup NativeHandle fd {}: {}", fd._base, e);
                    // SAFETY: OwnedFd requires the fd is not -1. If we failed to dup the fd,
                    // other code downstream will fail, but we can do no better than pass it on.
                    unsafe { OwnedFd::from_raw_fd(fd._base as i32) }
                }
            })
        };

        // First, we create a desc with the wrong type, because we cannot create one whole cloth of
        // our desired return type unless T implements Default. This Default requirement is
        // superfluous (T::default() is never called), so we then transmute to our desired type.
        let desc = MQDescriptor::<(), SynchronizedReadWrite>::default();
        // SAFETY: This transmute changes only the element type parameter of the MQDescriptor. The
        // layout of an MQDescriptor does not depend on T as T appears in it only in PhantomData.
        let mut desc: MQDescriptor<T, SynchronizedReadWrite> = unsafe { std::mem::transmute(desc) };
        // SAFETY: These slices are created out of the pointer and length pairs exposed by the
        // individual descFoo accessors, so we know they are valid pointer/lengths and point to
        // data that will continue to exist as long as the desc does.
        //
        // Calls to the descFoo accessors on erased_desc are sound because we know inner.dupeDesc
        // returns a valid pointer to a new heap-allocated ErasedMessageQueueDesc.
        let (grantors, fds, ints, quantum, flags) = unsafe {
            let grantors = slice_from_raw_parts_or_empty(
                descGrantors(erased_desc),
                descNumGrantors(erased_desc),
            );
            let fds = slice_from_raw_parts_or_empty(
                descHandleFDs(erased_desc),
                descHandleNumFDs(erased_desc),
            );
            let ints = slice_from_raw_parts_or_empty(
                descHandleInts(erased_desc),
                descHandleNumInts(erased_desc),
            );
            let quantum = descQuantum(erased_desc);
            let flags = descFlags(erased_desc);
            (grantors, fds, ints, quantum, flags)
        };
        let fds = fds.iter().map(scoped_to_parcel_fd).collect();
        let ints = ints.to_vec();
        desc.grantors = grantors.iter().map(grantor_to_rust).collect();
        desc.handle = NativeHandle { fds, ints };
        desc.quantum = quantum;
        desc.flags = flags;
        // SAFETY: we must free the desc returned by dupeDesc; the pointer was
        // just returned above so we know it is valid.
        unsafe { freeDesc(erased_desc) };
        desc
    }
}

/// Waiting for a zero duration is interpreted as waiting forever by the C++
/// readBlocking/writeBlocking calls. In Rust this kind of sentinel behavior is
/// a smell as the Option type is idiomatic, so bump 0ns timeouts to 1ns.
fn timeout_nanos(timeout: Option<Duration>) -> i64 {
    let nanos = timeout.map(|t| t.as_nanos().max(1)).unwrap_or(0);
    // Saturate to maximum i64 if nanos exceeds its range
    nanos.try_into().unwrap_or(i64::MAX)
}

#[test]
fn test_timeout_nanos() {
    assert_eq!(timeout_nanos(None), 0);
    assert_eq!(timeout_nanos(Some(Duration::from_nanos(0))), 1);
    assert_eq!(timeout_nanos(Some(Duration::from_nanos(1))), 1);
    assert_eq!(timeout_nanos(Some(Duration::from_nanos(5000))), 5000);
    assert_eq!(timeout_nanos(Some(Duration::from_nanos(i64::MAX as u64))), i64::MAX);
    assert_eq!(timeout_nanos(Some(Duration::from_nanos(i64::MAX as u64 + 1))), i64::MAX);
    assert_eq!(timeout_nanos(Some(Duration::from_nanos(u64::MAX))), i64::MAX);
}

impl<T: binder::WriteTo> MessageQueue<T> {
    /// Begin a write transaction. The returned WriteCompletion can be used to
    /// write into the region allocated for the transaction.
    pub fn write(&mut self) -> Option<WriteCompletion<T>> {
        self.write_many(1)
    }

    /// Begin a write transaction for multiple items. See `MessageQueue::write`.
    pub fn write_many(&mut self, n: usize) -> Option<WriteCompletion<T>> {
        let txn = self.begin_write(n)?;
        Some(WriteCompletion { inner: txn, queue: self, n_elems: n, n_written: 0 })
    }

    fn commit_write(&mut self, txn: MemTransaction) -> bool {
        // SAFETY: simply calls commitWrite with the txn length. The txn must
        // only use its first MemRegion.
        unsafe { self.inner.commitWrite(txn.first.length + txn.second.length) }
    }

    fn begin_write(&self, n: usize) -> Option<MemTransaction> {
        let mut txn: MemTransaction = Default::default();
        // SAFETY: we pass a raw pointer to txn, which is used only during the
        // call to beginWrite to write the txn's MemRegion fields, which are raw
        // pointers and lengths pointing into the queue. The pointer to txn is
        // not stored.
        unsafe { self.inner.beginWrite(n, addr_of_mut!(txn)) }.then_some(txn)
    }

    /// Write a slice of items into the `MessageQueue`, blocking until the
    /// entire slice can be written. If a non-`None` `timeout` is passed, the
    /// call will time out after this duration.
    ///
    /// The return value indicates whether the slice was written into the
    /// MessageQueue. If false is returned, no elements were written.
    pub fn write_blocking(&mut self, data: &[T], timeout: Option<Duration>) -> bool {
        // SAFETY: we pass the mutable slice as raw pointer and length, and the
        // pointer is known to be valid as it is derived from the slice
        // directly. For types implementing `Share`, we know that it is OK
        // to transfer the type over SHM in this manner.
        unsafe {
            self.inner.writeBlocking(data.as_ptr() as *const _, data.len(), timeout_nanos(timeout))
        }
    }
}

/// Forms a slice from a pointer and a length.
///
/// Returns an empty slice when `data` is a null pointer and `len` is zero.
///
/// # Safety
///
/// This function has the same safety requirements as [`std::slice::from_raw_parts`],
/// but unlike that function, does not exhibit undefined behavior when `data` is a
/// null pointer and `len` is zero. In this case, it returns an empty slice.
unsafe fn slice_from_raw_parts_or_empty<'a, T>(data: *const T, len: usize) -> &'a [T] {
    if data.is_null() && len == 0 {
        &[]
    } else {
        // SAFETY: The caller must guarantee to satisfy the safety requirements
        // of the standard library function [`std::slice::from_raw_parts`].
        unsafe { std::slice::from_raw_parts(data, len) }
    }
}

/// Obtain the address of the `idx`th element of the `MemTransaction`, which
/// may fall into either of its contiguous regions.
///
/// This pointer will be aligned to the size of the type `T`, as given by
/// `MessageQueue::<T>::type_size`, because the underlying `ErasedMessageQueue`
/// is created with that size as its `quantum`. Because a type's size is always
/// a multiple of its alignment, this means this pointer is correctly aligned
/// for type `T`.
#[inline(always)]
fn ptr<T>(txn: &MemTransaction, idx: usize) -> *mut T {
    let (base, region_idx) = if idx < txn.first.length {
        (txn.first.address, idx)
    } else {
        (txn.second.address, idx - txn.first.length)
    };
    let byte_count: usize = region_idx.checked_mul(MessageQueue::<T>::type_size()).unwrap();
    base.wrapping_byte_offset(byte_count.try_into().unwrap()) as *mut T
}

#[inline(always)]
fn contiguous_count(txn: &MemTransaction, idx: usize, n_elems: usize) -> usize {
    if idx >= n_elems {
        return 0;
    }

    if idx < txn.first.length {
        // In first region
        txn.first.length - idx
    } else {
        // In second region
        let idx_in_second_region = idx - txn.first.length;
        txn.second.length - idx_in_second_region
    }
}

/** A read completion from the MessageQueue::read() method.

This completion mutably borrows the MessageQueue to prevent concurrent reads;
these must be forbidden because the underlying AidlMessageQueue only stores the
number of outstanding reads, not which have and have not completed, so they
must complete in order. */
#[must_use]
pub struct ReadCompletion<'a, T: TryFromBytes> {
    inner: MemTransaction,
    queue: &'a mut MessageQueue<T>,
    n_elems: usize,
    n_read: usize,
}

/// The read instance of a type `T` was not a valid bit pattern. See
/// `zerocopy::error::ValidityError`, which this is like except that it does
/// not carry the source value (because it was copied into a temporary buffer).
#[derive(Eq, PartialEq, Debug)]
pub struct ValidityError<T>(PhantomData<T>);

impl<'a, T: TryFromBytes + 'a> ReadCompletion<'a, T> {
    /// Obtain a pointer to the location at which the idx'th item is located.
    ///
    /// The returned pointer is correctly aligned for type `T`.
    ///
    /// The returned pointer is only valid while `self` has not been dropped and
    /// is invalidated by any call to `self.read`. The pointer should be used
    /// with `std::ptr::read_volatile` or a DMA API before calling `assume_read`
    /// to indicate how many elements were read.
    ///
    /// It is only permitted to access at most `contiguous_count(idx)` items
    /// via offsets from the returned address.
    ///
    /// Calling this method with a greater `idx` may return a pointer to another
    /// memory region of different size than the first.
    pub fn ptr(&self, idx: usize) -> *mut T {
        if idx >= self.n_elems {
            panic!(
                "indexing out of bound: ReadCompletion for {} elements but idx {} accessed",
                self.n_elems, idx
            )
        }
        ptr(&self.inner, idx)
    }

    /// Return the number of contiguous elements located starting at the given
    /// index in the backing buffer corresponding to the given index.
    ///
    /// Intended for use with the `ptr` method.
    ///
    /// Returns 0 if `idx` is greater than or equal to the completion's element
    /// count.
    pub fn contiguous_count(&self, idx: usize) -> usize {
        contiguous_count(&self.inner, idx, self.n_elems)
    }

    /// Returns how many elements still must be read from `self` before dropping
    /// it.
    pub fn unread_elements(&self) -> usize {
        assert!(self.n_read <= self.n_elems);
        self.n_elems - self.n_read
    }

    /// Read one item from the `self`.
    ///
    /// Returns `None` if `self` is empty. Returns an error if a message was received, but it was
    /// invalid. (For example, an AIDL union with an illegal tag.)
    pub fn try_read(&mut self) -> Result<Option<T>, ValidityError<T>> {
        if self.unread_elements() == 0 {
            return Ok(None);
        }

        // SAFETY: `self.ptr(self.n_read)` returns a pointer aligned for the
        // type `T` into a shared-memory buffer that is legal to read with
        // volatile.
        let buf = unsafe { (self.ptr(self.n_read).cast::<MaybeUninit<T>>()).read_volatile() };

        self.n_read += 1;

        let ptr = ptr::from_ref(&buf) as *const u8;
        // SAFETY: Memory read from the shared-memory buffer is initialized.
        let bytes = unsafe { std::slice::from_raw_parts(ptr, size_of::<T>()) };

        let msg = T::try_read_from_bytes(bytes).map_err(|_| ValidityError(PhantomData))?;
        Ok(Some(msg))
    }

    /// Read one item from the `self`.
    ///
    /// Returns `None` if `self` is empty. Use `try_read` instead if the value does not implement
    /// `FromBytes`.
    pub fn read(&mut self) -> Option<T>
    where
        T: zerocopy::FromBytes,
    {
        if self.unread_elements() == 0 {
            return None;
        }

        // SAFETY: `self.ptr(self.n_read)` returns a pointer aligned for the
        // type `T` into a shared-memory buffer that is legal to read with
        // volatile.
        //
        // It is legal to interpret
        let res = unsafe { (self.ptr(self.n_read)).read_volatile() };
        self.n_read += 1;
        Some(res)
    }

    /// Promise to the `ReadCompletion` that `n_newly_read` elements have
    /// been read with unsafe code or DMA from the pointer returned by the
    /// `ptr` method.
    ///
    /// Panics if `n_newly_read` exceeds the number of elements still unread.
    ///
    /// Calling this method without actually reading the elements will result
    /// in them being leaked without destructors (if any) running.
    pub fn assume_read(&mut self, n_newly_read: usize) {
        assert!(n_newly_read <= self.unread_elements());
        self.n_read += n_newly_read;
    }
}

impl<T: TryFromBytes> Drop for ReadCompletion<'_, T> {
    fn drop(&mut self) {
        if self.n_read < self.n_elems {
            error!(
                "ReadCompletion dropped without reading all elements ({}/{} read)",
                self.n_read, self.n_elems
            );
        }
        let txn = std::mem::take(&mut self.inner);
        self.queue.commit_read(txn);
    }
}

impl<T: TryFromBytes> MessageQueue<T> {
    /// Begin a read transaction. The returned `ReadCompletion` can be used to
    /// write into the region allocated for the transaction.
    pub fn read(&mut self) -> Option<ReadCompletion<T>> {
        self.read_many(1)
    }

    /// Begin a read transaction for multiple items. See `MessageQueue::read`.
    pub fn read_many(&mut self, n: usize) -> Option<ReadCompletion<T>> {
        let txn = self.begin_read(n)?;
        Some(ReadCompletion { inner: txn, queue: self, n_elems: n, n_read: 0 })
    }

    fn commit_read(&mut self, txn: MemTransaction) -> bool {
        // SAFETY: simply calls commitRead with the txn length. The txn must
        // only use its first MemRegion.
        unsafe { self.inner.commitRead(txn.first.length + txn.second.length) }
    }

    fn begin_read(&self, n: usize) -> Option<MemTransaction> {
        let mut txn: MemTransaction = Default::default();
        // SAFETY: we pass a raw pointer to txn, which is used only during the
        // call to beginRead to write the txn's MemRegion fields, which are raw
        // pointers and lengths pointing into the queue. The pointer to txn is
        // not stored.
        unsafe { self.inner.beginRead(n, addr_of_mut!(txn)) }.then_some(txn)
    }

    /// Read a slice of items from the `MessageQueue`, blocking until the entire
    /// slice can be filled. If a non-`None` `timeout` is passed, the call will
    /// time out after this duration.
    ///
    /// The return value indicates whether the slice was filled with elements
    /// from the MessageQueue. If false is returned, the slice is not modified.
    pub fn read_blocking(&mut self, data: &mut [T], timeout: Option<Duration>) -> bool {
        // SAFETY: we pass the mutable slice as raw pointer and length, and the
        // pointer is known to be valid as it is derived from the slice
        // directly. For types implementing `Share`, we know that it is OK to
        // obtain an instance of the type by copying from SHM in this manner.
        unsafe {
            self.inner.readBlocking(data.as_mut_ptr() as *mut _, data.len(), timeout_nanos(timeout))
        }
    }

    /// Obtain a reference to the `MessageQueue`'s event flag word if it has one.
    ///
    /// Producers and consumers may synchronize over the status of the
    /// `MessageQueue`'s contents using this atomic, but the specific protocol
    /// for doing so is not specified and must be coordinated by developers of
    /// components that intend to use the `MessageQueue` for synchronization.
    pub fn event_flag_word(&self) -> Option<&AtomicU32> {
        // SAFETY: the FFI call only depends on the validity of `self`.
        // Casting a `*mut u32` to `*mut AtomicU32` is valid. The returned
        // pointer to the EventFlag word is either NULL or a pointer to an
        // allocation that lasts as long as the underlying FMQ does.
        unsafe {
            let event_flag_ptr = self.inner.getEventFlagWord();
            let atomic_ptr = event_flag_ptr as *mut AtomicU32;
            NonNull::new(atomic_ptr).map(|nn| nn.as_ref())
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn slice_from_raw_parts_or_empty_with_nonempty() {
        const SLICE: &[u8] = &[1, 2, 3, 4, 5, 6];
        // SAFETY: We are constructing a slice from the pointer and length of valid slice.
        let from_raw_parts = unsafe {
            let ptr = SLICE.as_ptr();
            let len = SLICE.len();
            slice_from_raw_parts_or_empty(ptr, len)
        };
        assert_eq!(SLICE, from_raw_parts);
    }

    #[test]
    fn slice_from_raw_parts_or_empty_with_null_pointer_zero_length() {
        // SAFETY: Calling `slice_from_raw_parts_or_empty` with a null pointer
        // and a zero length is explicitly allowed by its safety requirements.
        // In this case, `std::slice::from_raw_parts` has undefined behavior.
        let empty_from_raw_parts = unsafe {
            let ptr: *const u8 = std::ptr::null();
            let len = 0;
            slice_from_raw_parts_or_empty(ptr, len)
        };
        assert_eq!(&[] as &[u8], empty_from_raw_parts);
    }

    #[test]
    fn contiguous_count_index_out_of_bounds_returns_zero() {
        let mut txn: MemTransaction = Default::default();
        txn.first.length = 10;
        txn.second.length = 5;
        let n_elems = txn.first.length + txn.second.length;

        assert_eq!(contiguous_count(&txn, n_elems + 1, n_elems), 0);
        assert_eq!(contiguous_count(&txn, n_elems, n_elems), 0);
    }

    #[test]
    fn contiguous_count_index_in_first_region_returns_correct_count() {
        let mut txn: MemTransaction = Default::default();
        txn.first.length = 10;
        txn.second.length = 5;
        let n_elems = txn.first.length + txn.second.length;

        assert_eq!(contiguous_count(&txn, 0, n_elems), 10);
        assert_eq!(contiguous_count(&txn, 5, n_elems), 5);
        assert_eq!(contiguous_count(&txn, 9, n_elems), 1);
    }

    #[test]
    fn contiguous_count_index_at_first_region_boundary_returns_second_region_length() {
        let mut txn: MemTransaction = Default::default();
        txn.first.length = 10;
        txn.second.length = 5;
        let n_elems = txn.first.length + txn.second.length;

        assert_eq!(contiguous_count(&txn, 10, n_elems), 5);
    }

    #[test]
    fn contiguous_count_index_in_second_region_returns_correct_count() {
        let mut txn: MemTransaction = Default::default();
        txn.first.length = 10;
        txn.second.length = 5;
        let n_elems = txn.first.length + txn.second.length;

        assert_eq!(contiguous_count(&txn, 11, n_elems), 4);
        assert_eq!(contiguous_count(&txn, 14, n_elems), 1);
    }
}
