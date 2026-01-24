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

//! Crate to wrap tests of libfmq rust bindings with a trivial C-ABI interface
//! to test them from C++.

use fmq::{MessageQueue, WriteCompletion};

macro_rules! assert_return {
    ($e: expr) => {
        if !$e {
            eprintln!(stringify!($e));
            return false;
        }
    };
    ($e: expr, $msg: expr) => {
        if !$e {
            eprintln!($msg);
            return false;
        }
    };
}

/// Verifies that after a `write_many`, `read_many` can be used to read parts of the written data
/// from different threads/queue descriptors.
fn read_many_reads_after_write_many_test() -> bool {
    let mut mq = MessageQueue::<u8>::new(500, false);

    match mq.write_many(4) {
        Some(mut wc) => {
            wc.write(200).unwrap();
            wc.write(201).unwrap();
            wc.write(202).unwrap();
            wc.write(203).unwrap();
        }
        None => {
            eprintln!("failed to write_many(4)");
            return false;
        }
    };

    let desc = mq.dupe_desc();
    let join_handle = std::thread::spawn(move || {
        let mut mq2 = MessageQueue::from_desc(&desc, false);
        match mq2.read_many(1) {
            Some(mut rc) => {
                assert_return!(rc.read() == Some(200));
            }
            None => {
                eprintln!("failed to read_many(1)");
                return false;
            }
        };
        true
    });

    assert_return!(join_handle.join().ok() == Some(true));

    match mq.read_many(3) {
        Some(mut rc) => {
            assert_return!(rc.read() == Some(201));
            assert_return!(rc.read() == Some(202));
            assert_return!(rc.read() == Some(203));
            drop(rc);
        }
        None => {
            eprintln!("failed to read_many(4)");
            return false;
        }
    };

    true
}

/// Helper to write a contiguous block of data. Returns false if there is not
/// enough contiguous space.
///
/// This also calls `assume_written` for the length of `data` on success.
fn write_contiguous(writer: &mut WriteCompletion<u8>, write_offset: usize, data: &[u8]) -> bool {
    if writer.contiguous_count(write_offset) < data.len() {
        return false;
    }

    let buffer_ptr = writer.ptr(write_offset);
    // SAFETY: ptr() gives a valid pointer, and we are writing `data.len()` bytes
    // which is within the `contiguous` block available.
    //`u8` has no alignment requirements and can be safely copied.
    // Unsafe assume_written is called after writing data using the pointer returned by ptr().
    unsafe {
        std::ptr::copy_nonoverlapping(data.as_ptr(), buffer_ptr, data.len());
        writer.assume_written(data.len());
    }
    true
}

/// Verifies writing to the queue via pointers when the write does not wrap around the buffer.
/// In that scenario we only need to write one contiguous segment of the buffer.
fn write_by_pointer_no_wraparound_test() -> bool {
    const TEST_SIZE: usize = 100;
    let mut mq = MessageQueue::<u8>::new(TEST_SIZE, false);

    let data_to_write: Vec<u8> = (0..TEST_SIZE as u8 / 2).collect();

    match mq.write_many(data_to_write.len()) {
        Some(mut writer) => {
            // Only one contiguous segment expected as we writing half of the buffer from the start.
            assert_return!(write_contiguous(&mut writer, 0, &data_to_write));
        }
        None => {
            eprintln!("failed to write_many");
            return false;
        }
    }

    // Check that we can read the data as it was written.
    let mut read_data = Vec::with_capacity(data_to_write.len());
    match mq.read_many(data_to_write.len()) {
        Some(mut reader) => {
            for _ in 0..data_to_write.len() {
                match reader.try_read() {
                    Ok(Some(val)) => read_data.push(val),
                    Err(_) => {
                        eprintln!("read invalid value for type");
                        return false;
                    }
                    Ok(None) => {
                        eprintln!("failed to read expected value");
                        return false;
                    }
                }
            }
        }
        None => {
            eprintln!("failed to read_many");
            return false;
        }
    }

    assert_return!(data_to_write == read_data);

    true
}

/// Verifies writing to the queue via pointers when the write wraps around the buffer.
/// In that scenario we need to write two contiguous segments of the buffer.
fn write_by_pointer_wraparound_test() -> bool {
    const TEST_SIZE: usize = 100;
    let mut mq = MessageQueue::<u8>::new(TEST_SIZE, false);

    // Write and read half the buffer to force a wraparound on next write.
    let half_size = TEST_SIZE / 2;
    assert_return!(mq.write_many(half_size).is_some());
    assert_return!(mq.read_many(half_size).is_some());

    let data_to_write: Vec<u8> = (0..TEST_SIZE as u8).collect();

    match mq.write_many(TEST_SIZE) {
        Some(mut writer) => {
            let first_contiguous = writer.contiguous_count(0);
            assert_return!(first_contiguous < TEST_SIZE);
            // Write first part of the buffer.
            assert_return!(write_contiguous(&mut writer, 0, &data_to_write[..first_contiguous]));

            // Write second part of the buffer.
            assert_return!(write_contiguous(
                &mut writer,
                first_contiguous,
                &data_to_write[first_contiguous..],
            ));
        }
        None => {
            eprintln!("failed to write_many");
            return false;
        }
    }

    // Check that we can read the data as it was written.
    let mut read_data = Vec::with_capacity(TEST_SIZE);
    match mq.read_many(TEST_SIZE) {
        Some(mut reader) => {
            for _ in 0..TEST_SIZE {
                match reader.try_read() {
                    Ok(Some(val)) => read_data.push(val),
                    Err(_) => {
                        eprintln!("read invalid value for type");
                        return false;
                    }
                    Ok(None) => {
                        eprintln!("failed to read expected value");
                        return false;
                    }
                }
            }
        }
        None => {
            eprintln!("failed to read_many");
            return false;
        }
    }

    assert_return!(data_to_write == read_data);

    true
}

/// Test fmq from Rust. Returns 0 on failure, 1 on success.
#[no_mangle]
pub extern "C" fn fmq_rust_test() -> u8 {
    (read_many_reads_after_write_many_test()
        && write_by_pointer_no_wraparound_test()
        && write_by_pointer_wraparound_test()) as u8
}
