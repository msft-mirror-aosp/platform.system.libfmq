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

use android_fmq_test::aidl::android::fmq::test::ITestAidlMsgQ::{
    EnumPayload::EnumPayload, ITestAidlMsgQ, StructPayload::StructPayload,
    UnionPayload::UnionPayload,
};
use binder::{Result as BinderResult, Strong};
use fmq::{MQDescriptor, MessageQueue, SynchronizedReadWrite};
use nix::unistd::{sysconf, SysconfVar};

fn wait_get_test_service() -> Result<Strong<dyn ITestAidlMsgQ>, String> {
    const SERVICE_IDENTIFIER: &str = "android.fmq.test.ITestAidlMsgQ/default";
    let service = binder::get_interface::<dyn ITestAidlMsgQ>(SERVICE_IDENTIFIER)
        .map_err(|e| format!("Failed to connect to service {SERVICE_IDENTIFIER}: {e}"))?;
    Ok(service)
}

trait FmqTestPayload: zerocopy::TryFromBytes + binder::WriteTo + Sized {
    fn init_data<const N: usize>() -> [Self; N];
    fn configure_service(
        service: &dyn ITestAidlMsgQ,
        desc: &MQDescriptor<Self, SynchronizedReadWrite>,
    ) -> BinderResult<bool>;
}

impl FmqTestPayload for i32 {
    fn init_data<const N: usize>() -> [Self; N] {
        let mut data = [0; N];
        for (i, elem) in data.iter_mut().enumerate() {
            *elem = i as _;
        }
        data
    }
    fn configure_service(
        service: &dyn ITestAidlMsgQ,
        desc: &MQDescriptor<Self, SynchronizedReadWrite>,
    ) -> BinderResult<bool> {
        service.configureFmqSyncReadWrite(desc)
    }
}
impl FmqTestPayload for StructPayload {
    fn init_data<const N: usize>() -> [Self; N] {
        [Self::default(); N] // TODO: vary StructPayload element values
    }
    fn configure_service(
        service: &dyn ITestAidlMsgQ,
        desc: &MQDescriptor<Self, SynchronizedReadWrite>,
    ) -> BinderResult<bool> {
        service.configureFmqAidlTypesSyncReadWrite(Some(desc), None, None)
    }
}
impl FmqTestPayload for UnionPayload {
    fn init_data<const N: usize>() -> [Self; N] {
        [Self::default(); N] // TODO: vary UnionPayload element values
    }
    fn configure_service(
        service: &dyn ITestAidlMsgQ,
        desc: &MQDescriptor<Self, SynchronizedReadWrite>,
    ) -> BinderResult<bool> {
        service.configureFmqAidlTypesSyncReadWrite(None, Some(desc), None)
    }
}
impl FmqTestPayload for EnumPayload {
    fn init_data<const N: usize>() -> [Self; N] {
        // Fill data with repeating sequence of Variants 1, 2, 3.
        let mut data = [EnumPayload::VARIANT1; N];
        let cycle = [EnumPayload::VARIANT1, EnumPayload::VARIANT2, EnumPayload::VARIANT3]
            .into_iter()
            .cycle();
        for (val, elem) in cycle.zip(data.iter_mut()) {
            *elem = val;
        }
        data
    }
    fn configure_service(
        service: &dyn ITestAidlMsgQ,
        desc: &MQDescriptor<Self, SynchronizedReadWrite>,
    ) -> BinderResult<bool> {
        service.configureFmqAidlTypesSyncReadWrite(None, None, Some(desc))
    }
}

fn setup_test_service<T: FmqTestPayload>() -> (MessageQueue<T>, Strong<dyn ITestAidlMsgQ>) {
    let service = wait_get_test_service().expect("failed to obtain test service");

    let page_size: usize = sysconf(SysconfVar::PAGE_SIZE)
        .expect("sysconf(PAGE_SIZE) failed")
        .unwrap()
        .try_into()
        .expect("PAGE_SIZE out of bounds for usize");
    let num_elements_in_sync_queue: usize = (page_size - 16) / std::mem::size_of::<T>();

    /* Create a queue on the client side. */
    let mq = MessageQueue::<T>::new(
        num_elements_in_sync_queue,
        true, /* configure event flag word */
    );
    let desc = mq.dupe_desc();

    let result = T::configure_service(&*service, &desc);
    assert!(result.is_ok(), "configuring event queue failed");

    (mq, service)
}

mod synchronized_read_write_client {
    use super::*;

    #[cfg(test)]
    fn test_small_input_write<T: FmqTestPayload + std::fmt::Debug>() {
        let (mut mq, service) = setup_test_service();
        const DATA_LEN: usize = 16;

        let data: [T; DATA_LEN] = T::init_data();
        let mut wc = mq.write_many(DATA_LEN).expect("write_many(DATA_LEN) failed");
        for x in data {
            wc.write(x).expect("writing failed");
        }
        drop(wc);
        let ret = service.requestReadFmqSync(DATA_LEN as _);
        assert!(ret.is_ok(), "requestReadFmqSync: {ret:?}");
    }

    /*
     * Write a small number of messages to an FMQ. Requests
     * mService to read and verify that the write was successful.
     *
     * Repeats this for various types.
     */
    #[test]
    fn small_input_writer_test_i32() {
        test_small_input_write::<i32>();
        test_small_input_write::<StructPayload>();
        test_small_input_write::<UnionPayload>();
        test_small_input_write::<EnumPayload>();
    }
}

// TODO: use similar logic to vary values of all types
fn _init_data<const N: usize>() -> [i32; N] {
    let mut data = [0; N];
    for (i, elem) in data.iter_mut().enumerate() {
        *elem = i as _;
    }
    data
}
