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

use android_fmq_test::aidl::android::fmq::test::{
    EventFlagBits::EventFlagBits, ITestAidlMsgQ::ITestAidlMsgQ,
};
use android_fmq_test::binder::{self, Interface, Result as BinderResult};

/// Struct implementing the ITestAidlMsgQ AIDL interface
#[derive(Default)]
pub struct MsgQTestService {
    queue_sync: std::sync::Mutex<Option<fmq::MessageQueue<i32>>>,
}

impl Interface for MsgQTestService {}

use android_hardware_common_fmq::aidl::android::hardware::common::fmq::{
    MQDescriptor::MQDescriptor, SynchronizedReadWrite::SynchronizedReadWrite,
    UnsynchronizedWrite::UnsynchronizedWrite,
};

use std::sync::atomic::Ordering;

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
        let mq = fmq::MessageQueue::from_desc(mq_desc, true);
        /* Set the EventFlag word with bit FMQ_NOT_FULL. */
        if let Some(event_word) = mq.event_flag_word() {
            event_word.store(EventFlagBits::FMQ_NOT_FULL.0 as u32, Ordering::Relaxed);
        }
        *self.queue_sync.lock().unwrap() = Some(mq);

        Ok(true)
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
        let mut queue_guard = self.queue_sync.lock().unwrap();
        let Some(ref mut mq) = *queue_guard else {
            return Err(binder::Status::new_service_specific_error_str(107, Some("no fmq set up")));
        };
        let rc = mq.read_many(count.try_into().unwrap());
        match rc {
            Some(mut rc) => {
                for _ in 0..count {
                    rc.read().unwrap();
                }
                Ok(true)
            }
            None => {
                eprintln!("failed to read_many({count})");
                Ok(false)
            }
        }
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
        let mut queue_guard = self.queue_sync.lock().unwrap();
        let Some(ref mut mq) = *queue_guard else {
            return Err(binder::Status::new_service_specific_error_str(107, Some("no fmq set up")));
        };
        let wc = mq.write_many(count.try_into().unwrap());
        match wc {
            Some(mut wc) => {
                for i in 0..count {
                    wc.write(i).unwrap();
                }
                drop(wc);
                Ok(true)
            }
            None => {
                eprintln!("failed to write_many({count})");
                Ok(false)
            }
        }
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
