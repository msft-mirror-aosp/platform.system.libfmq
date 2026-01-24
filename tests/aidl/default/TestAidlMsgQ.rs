//! This module implements the ITestAidlMsgQ AIDL interface

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

use std::{fmt::Debug, sync::Mutex};

use android_fmq_test::aidl::android::fmq::test::{
    EventFlagBits::EventFlagBits,
    ITestAidlMsgQ::{
        EnumPayload::EnumPayload, ITestAidlMsgQ, StructPayload::StructPayload,
        UnionPayload::UnionPayload,
    },
};
use android_fmq_test::binder::{self, Interface, Result as BinderResult};

/// Struct implementing the ITestAidlMsgQ AIDL interface
#[derive(Default)]
pub struct MsgQTestService {
    queue_sync_struct: Mutex<Option<fmq::MessageQueue<StructPayload>>>,
    queue_sync_union: Mutex<Option<fmq::MessageQueue<UnionPayload>>>,
    queue_sync_enum: Mutex<Option<fmq::MessageQueue<EnumPayload>>>,
    queue_sync: Mutex<Option<fmq::MessageQueue<i32>>>,
}

impl Interface for MsgQTestService {}

use android_hardware_common_fmq::aidl::android::hardware::common::fmq::{
    MQDescriptor::MQDescriptor, SynchronizedReadWrite::SynchronizedReadWrite,
    UnsynchronizedWrite::UnsynchronizedWrite,
};

use std::sync::atomic::Ordering;

/// Create a `MessageQueue` from the given descriptor, configuring an event flag
/// word and initializing it to `EventFlagBits::FMQ_NOT_FULL`.
fn mq_with_ev_flag_word<T>(desc: &MQDescriptor<T, SynchronizedReadWrite>) -> fmq::MessageQueue<T>
where
    T: zerocopy::TryFromBytes,
{
    let mq = fmq::MessageQueue::from_desc(desc, true);
    /* Set the EventFlag word with bit FMQ_NOT_FULL. */
    if let Some(event_word) = mq.event_flag_word() {
        event_word.store(EventFlagBits::FMQ_NOT_FULL.0 as u32, Ordering::Relaxed);
    }
    mq
}

impl ITestAidlMsgQ for MsgQTestService {
    /**
     * This method requests the service to set up a synchronous read/write
     * wait-free FMQ using the input descriptor with the client as reader.
     *
     * @param mqDesc This structure describes the FMQ that was set up by the
     * client. Server uses this descriptor to set up a FMQ object at its end.
     *
     * @return True if the setup is successful.
     */
    fn configureFmqSyncReadWrite(
        &self,
        mq_desc: &MQDescriptor<i32, SynchronizedReadWrite>,
    ) -> BinderResult<bool> {
        *self.queue_sync_struct.lock().unwrap() = None;
        *self.queue_sync_union.lock().unwrap() = None;
        *self.queue_sync_enum.lock().unwrap() = None;
        *self.queue_sync.lock().unwrap() = Some(mq_with_ev_flag_word(mq_desc));

        Ok(true)
    }

    /**
     * This method requests the service to set up a synchronous read/write
     * wait-free FMQ using the given input descriptor with client as reader.
     *
     * Only exactly one of the arguments should be non-NULL. The server will use
     * the non-NULL descriptor to set up a FMQ object at its end, which can then
     * be directed to read or write with the various request* methods.
     *
     * @return True if the setup is successful.
     */
    fn configureFmqAidlTypesSyncReadWrite(
        &self,
        mq_desc_struct: Option<&MQDescriptor<StructPayload, SynchronizedReadWrite>>,
        mq_desc_union: Option<&MQDescriptor<UnionPayload, SynchronizedReadWrite>>,
        mq_desc_enum: Option<&MQDescriptor<EnumPayload, SynchronizedReadWrite>>,
    ) -> BinderResult<bool> {
        assert!(
            [mq_desc_struct.is_some(), mq_desc_union.is_some(), mq_desc_enum.is_some(),]
                .into_iter()
                .filter(|b| *b)
                .count()
                == 1,
            "exactly one descriptor must be non-NULL"
        );
        *self.queue_sync_struct.lock().unwrap() = mq_desc_struct.map(mq_with_ev_flag_word);
        *self.queue_sync_union.lock().unwrap() = mq_desc_union.map(mq_with_ev_flag_word);
        *self.queue_sync_enum.lock().unwrap() = mq_desc_enum.map(mq_with_ev_flag_word);
        *self.queue_sync.lock().unwrap() = None;
        Ok(true)
    }
    fn getFmqAidlTypesUnsyncWrite(
        &self,
        _: bool,
        _: bool,
        _: &mut Option<MQDescriptor<StructPayload, UnsynchronizedWrite>>,
        _: &mut Option<MQDescriptor<UnionPayload, UnsynchronizedWrite>>,
        _: &mut Option<MQDescriptor<EnumPayload, UnsynchronizedWrite>>,
    ) -> BinderResult<bool> {
        // The Rust interface to FMQ does not support `UnsynchronizedWrite`.
        Ok(false)
    }

    /**
     * This method requests the service to read from the synchronized read/write
     * FMQ.
     *
     * @param count Number to messages to read.
     *
     * @return True if the read operation was successful.
     */
    fn requestReadFmqSync(&self, count: i32) -> BinderResult<bool> {
        fn read_sync<T: zerocopy::TryFromBytes + Debug>(
            mutex_mq: &Mutex<Option<fmq::MessageQueue<T>>>,
            count: i32,
        ) -> Option<bool> {
            let mut queue_guard = mutex_mq.lock().unwrap();
            let mq = queue_guard.as_mut()?;
            let rc = mq.read_many(count.try_into().unwrap());
            match rc {
                Some(mut rc) => {
                    for _ in 0..count {
                        rc.try_read().unwrap().unwrap();
                    }
                    Some(true)
                }
                None => {
                    eprintln!("failed to read_many({count})");
                    Some(false)
                }
            }
        }

        if let Some(ret) = read_sync(&self.queue_sync_struct, count) {
            return Ok(ret);
        }
        if let Some(ret) = read_sync(&self.queue_sync_union, count) {
            return Ok(ret);
        }
        if let Some(ret) = read_sync(&self.queue_sync_enum, count) {
            return Ok(ret);
        }
        if let Some(ret) = read_sync(&self.queue_sync, count) {
            return Ok(ret);
        }

        Err(binder::Status::new_service_specific_error_str(107, Some("no fmq set up")))
    }

    /**
     * This method requests the service to write into the synchronized read/write
     * flavor of the FMQ.
     *
     * @param count Number to messages to write.
     *
     * @return True if the write operation was successful.
     */
    fn requestWriteFmqSync(&self, count: i32) -> BinderResult<bool> {
        fn write_sync<T: binder::WriteTo + zerocopy::Immutable + Debug, F: Fn(i32) -> T>(
            mutex_mq: &Mutex<Option<fmq::MessageQueue<T>>>,
            count: i32,
            gen_val: F,
        ) -> Option<bool> {
            let mut queue_guard = mutex_mq.lock().unwrap();
            let mq = queue_guard.as_mut()?;
            let wc = mq.write_many(count.try_into().unwrap());
            match wc {
                Some(mut wc) => {
                    for i in 0..count {
                        wc.write(gen_val(i)).unwrap();
                    }
                    drop(wc);
                    Some(true)
                }
                None => {
                    eprintln!("failed to write_many({count})");
                    Some(false)
                }
            }
        }

        if let Some(ret) = write_sync(&self.queue_sync_struct, count, |_: i32| Default::default()) {
            return Ok(ret);
        }
        if let Some(ret) = write_sync(&self.queue_sync_union, count, |_: i32| Default::default()) {
            return Ok(ret);
        }
        if let Some(ret) = write_sync(&self.queue_sync_enum, count, |_: i32| Default::default()) {
            return Ok(ret);
        }
        if let Some(ret) = write_sync(&self.queue_sync, count, move |i: i32| i) {
            return Ok(ret);
        }

        Err(binder::Status::new_service_specific_error_str(107, Some("no fmq set up")))
    }

    fn getFmqUnsyncWrite(
        &self,
        _: bool,
        _: bool,
        _: &mut MQDescriptor<i32, UnsynchronizedWrite>,
    ) -> BinderResult<bool> {
        // The Rust interface to FMQ does not support `UnsynchronizedWrite`.
        Ok(false)
    }

    /**
     * This method requests the service to trigger a blocking read.
     *
     * @param count Number of messages to read.
     *
     */
    fn requestBlockingRead(&self, count: i32) -> BinderResult<()> {
        self.requestBlockingReadDefaultEventFlagBits(count)
    }

    /**
     * This method requests the service to trigger a blocking read using
     * default Event Flag notification bits defined by the MessageQueue class.
     *
     * @param count Number of messages to read.
     *
     */
    fn requestBlockingReadDefaultEventFlagBits(&self, count: i32) -> BinderResult<()> {
        let mut queue_guard = self.queue_sync.lock().unwrap();
        let Some(ref mut mq) = *queue_guard else {
            return Err(binder::Status::new_service_specific_error_str(107, Some("no fmq set up")));
        };

        let mut buf = vec![0; count as usize];
        let result = mq.read_blocking(&mut buf[..], Some(std::time::Duration::from_secs(5)));

        if !result {
            return Err(binder::Status::new_service_specific_error_str(
                5,
                Some("blocking read failed"),
            ));
        }
        Ok(())
    }

    /**
     * This method requests the service to repeatedly trigger blocking reads.
     *
     * @param count Number of messages to read in a single blocking read.
     * @param numIter Number of blocking reads to trigger.
     *
     */
    fn requestBlockingReadRepeat(&self, count: i32, repeats: i32) -> BinderResult<()> {
        for _ in 0..repeats {
            self.requestBlockingRead(count)?;
        }
        Ok(())
    }

    fn requestReadFmqUnsync(&self, _: i32) -> BinderResult<bool> {
        // The Rust interface to FMQ does not support `UnsynchronizedWrite`.
        Ok(false)
    }
    fn requestWriteFmqUnsync(&self, _: i32) -> BinderResult<bool> {
        // The Rust interface to FMQ does not support `UnsynchronizedWrite`.
        Ok(false)
    }
}
