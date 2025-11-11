/*
 * Copyright (C) 2020 The Android Open Source Project
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

#pragma once

#include <aidl/android/fmq/test/BnTestAidlMsgQ.h>
#include <aidl/android/fmq/test/EventFlagBits.h>
#include <fmq/AidlMessageQueue.h>
#include <fmq/EventFlag.h>

namespace aidl {
namespace android {
namespace fmq {
namespace test {

using ::aidl::android::fmq::test::EventFlagBits;
using ::aidl::android::fmq::test::ITestAidlMsgQ;

using ::aidl::android::hardware::common::fmq::MQDescriptor;
using ::aidl::android::hardware::common::fmq::SynchronizedReadWrite;
using ::aidl::android::hardware::common::fmq::UnsynchronizedWrite;
using ::android::hardware::kSynchronizedReadWrite;
using ::android::hardware::kUnsynchronizedWrite;
using ::android::hardware::MQFlavor;

using ::android::AidlMessageQueue;

struct TestAidlMsgQ : public BnTestAidlMsgQ {
    using ::aidl::android::fmq::test::ITestAidlMsgQ::EnumPayload;
    using ::aidl::android::fmq::test::ITestAidlMsgQ::StructPayload;
    using ::aidl::android::fmq::test::ITestAidlMsgQ::UnionPayload;

    template <typename T>
    using MessageQueueSync = AidlMessageQueue<T, SynchronizedReadWrite>;
    template <typename T>
    using MessageQueueUnsync = AidlMessageQueue<T, UnsynchronizedWrite>;

    TestAidlMsgQ()
        : mFmqSynchronized(nullptr),
          mFmqSynchronizedStruct(nullptr),
          mFmqSynchronizedUnion(nullptr),
          mFmqSynchronizedEnum(nullptr),
          mFmqUnsynchronized(nullptr),
          mFmqUnsynchronizedStruct(nullptr),
          mFmqUnsynchronizedUnion(nullptr),
          mFmqUnsynchronizedEnum(nullptr) {}

    // Methods from ::aidl::android::fmq::test::ITestAidlMsgQ follow.
    ndk::ScopedAStatus configureFmqSyncReadWrite(
            const MQDescriptor<int32_t, SynchronizedReadWrite>& mqDesc,
            bool* _aidl_return) override;
    ndk::ScopedAStatus getFmqUnsyncWrite(bool configureFmq, bool userFd,
                                         MQDescriptor<int32_t, UnsynchronizedWrite>* mqDesc,
                                         bool* _aidl_return) override;
    ndk::ScopedAStatus configureFmqAidlTypesSyncReadWrite(
            const std::optional<MQDescriptor<StructPayload, SynchronizedReadWrite>>&
                    in_mqDescStruct,
            const std::optional<MQDescriptor<UnionPayload, SynchronizedReadWrite>>& in_mqDescUnion,
            const std::optional<MQDescriptor<EnumPayload, SynchronizedReadWrite>>& in_mqDescEnum,
            bool* _aidl_return) override;
    ndk::ScopedAStatus getFmqAidlTypesUnsyncWrite(
            bool in_configureFmq, bool in_userFd,
            std::optional<MQDescriptor<StructPayload, UnsynchronizedWrite>>* out_mqDescStruct,
            std::optional<MQDescriptor<UnionPayload, UnsynchronizedWrite>>* out_mqDescUnion,
            std::optional<MQDescriptor<EnumPayload, UnsynchronizedWrite>>* out_mqDescEnum,
            bool* _aidl_return) override;
    ndk::ScopedAStatus requestBlockingRead(int32_t count) override;
    ndk::ScopedAStatus requestBlockingReadDefaultEventFlagBits(int32_t count) override;
    ndk::ScopedAStatus requestBlockingReadRepeat(int32_t count, int32_t numIter) override;
    ndk::ScopedAStatus requestReadFmqSync(int32_t count, bool* _aidl_return) override;
    ndk::ScopedAStatus requestReadFmqUnsync(int32_t count, bool* _aidl_return) override;
    ndk::ScopedAStatus requestWriteFmqSync(int32_t count, bool* _aidl_return) override;
    ndk::ScopedAStatus requestWriteFmqUnsync(int32_t count, bool* _aidl_return) override;

  private:
    std::unique_ptr<MessageQueueSync<int32_t>> mFmqSynchronized;
    std::unique_ptr<MessageQueueSync<StructPayload>> mFmqSynchronizedStruct;
    std::unique_ptr<MessageQueueSync<UnionPayload>> mFmqSynchronizedUnion;
    std::unique_ptr<MessageQueueSync<EnumPayload>> mFmqSynchronizedEnum;
    std::unique_ptr<MessageQueueUnsync<int32_t>> mFmqUnsynchronized;
    std::unique_ptr<MessageQueueUnsync<StructPayload>> mFmqUnsynchronizedStruct;
    std::unique_ptr<MessageQueueUnsync<UnionPayload>> mFmqUnsynchronizedUnion;
    std::unique_ptr<MessageQueueUnsync<EnumPayload>> mFmqUnsynchronizedEnum;

    /*
     * Utility function to verify data read from the fast message queue.
     */
    bool verifyData(int32_t* data, int count) {
        for (int i = 0; i < count; i++) {
            if (data[i] != i) return false;
        }
        return true;
    }

    // TODO: implement verifyData analogue for types other than int32_t
};

}  // namespace test
}  // namespace fmq
}  // namespace android
}  // namespace aidl
