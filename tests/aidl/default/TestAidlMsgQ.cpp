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

#include "TestAidlMsgQ.h"

namespace aidl {
namespace android {
namespace fmq {
namespace test {

template <typename T>
ndk::ScopedAStatus configureFmqSyncReadWriteGeneric(
        const MQDescriptor<T, SynchronizedReadWrite>& desc,
        std::unique_ptr<TestAidlMsgQ::MessageQueueSync<T>>& fmq) {
    fmq.reset(new (std::nothrow) TestAidlMsgQ::MessageQueueSync(desc));
    if ((fmq == nullptr) || (fmq->isValid() == false)) {
        return ndk::ScopedAStatus::fromExceptionCode(EX_ILLEGAL_ARGUMENT);
    }

    /* Initialize the EventFlag word with bit FMQ_NOT_FULL. */
    auto evFlagWordPtr = fmq->getEventFlagWord();
    if (evFlagWordPtr != nullptr) {
        std::atomic_init(evFlagWordPtr, static_cast<uint32_t>(EventFlagBits::FMQ_NOT_FULL));
    }
    return ndk::ScopedAStatus::ok();
}

template <typename T>
bool getFmqUnsyncWriteGeneric(bool configureFmq, bool userFd,
                              MQDescriptor<T, UnsynchronizedWrite>* mqDesc,
                              std::unique_ptr<TestAidlMsgQ::MessageQueueUnsync<T>>& fmqUnsync) {
    if (configureFmq) {
        static constexpr size_t kNumElementsInQueue = 1024;
        static constexpr size_t kElementSizeBytes = sizeof(int32_t);
        ::android::base::unique_fd ringbufferFd;
        if (userFd) {
            ringbufferFd.reset(
                    ::ashmem_create_region("UnsyncWrite", kNumElementsInQueue * kElementSizeBytes));
        }
        fmqUnsync.reset(new (std::nothrow) TestAidlMsgQ::MessageQueueUnsync<T>(
                kNumElementsInQueue, false, std::move(ringbufferFd),
                kNumElementsInQueue * kElementSizeBytes));
    }

    if ((fmqUnsync == nullptr) || (fmqUnsync->isValid() == false) || (mqDesc == nullptr)) {
        return false;
    } else {
        *mqDesc = fmqUnsync->dupeDesc();
        // set write-protection so readers can't mmap and write
        int res = ashmem_set_prot_region(mqDesc->handle.fds[0].get(), PROT_READ);
        if (res == -1) {
            ALOGE("Failed to set write protection: %s", strerror(errno));
            return false;
        } else {
            return true;
        }
    }
}

// Methods from ::aidl::android::fmq::test::ITestAidlMsgQ follow.
ndk::ScopedAStatus TestAidlMsgQ::configureFmqSyncReadWrite(
        const MQDescriptor<int32_t, SynchronizedReadWrite>& mqDesc, bool* _aidl_return) {
    auto status = configureFmqSyncReadWriteGeneric(mqDesc, mFmqSynchronized);
    *_aidl_return = true;
    return status;
}

ndk::ScopedAStatus TestAidlMsgQ::getFmqUnsyncWrite(
        bool configureFmq, bool userFd, MQDescriptor<int32_t, UnsynchronizedWrite>* mqDesc,
        bool* _aidl_return) {
    *_aidl_return = getFmqUnsyncWriteGeneric(configureFmq, userFd, mqDesc, mFmqUnsynchronized);
    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::configureFmqAidlTypesSyncReadWrite(
        const std::optional<MQDescriptor<ITestAidlMsgQ::StructPayload, SynchronizedReadWrite>>&
                in_mqDescStruct,
        const std::optional<MQDescriptor<ITestAidlMsgQ::UnionPayload, SynchronizedReadWrite>>&
                in_mqDescUnion,
        const std::optional<MQDescriptor<ITestAidlMsgQ::EnumPayload, SynchronizedReadWrite>>&
                in_mqDescEnum,
        bool* _aidl_return) {
    /* enforce that exactly one argument is non-empty. */
    int n_nonempty =
            in_mqDescStruct.has_value() + in_mqDescUnion.has_value() + in_mqDescEnum.has_value();
    if (n_nonempty != 1) {
        return ndk::ScopedAStatus::fromExceptionCode(EX_ILLEGAL_ARGUMENT);
    }

    ndk::ScopedAStatus status;
    if (in_mqDescStruct.has_value()) {
        status = configureFmqSyncReadWriteGeneric(*in_mqDescStruct, mFmqSynchronizedStruct);
    }
    if (in_mqDescUnion.has_value()) {
        status = configureFmqSyncReadWriteGeneric(*in_mqDescUnion, mFmqSynchronizedUnion);
    }
    if (in_mqDescEnum.has_value()) {
        status = configureFmqSyncReadWriteGeneric(*in_mqDescEnum, mFmqSynchronizedEnum);
    }

    if (status.isOk()) {
        *_aidl_return = true;
    }
    return status;
}

// TODO: verify that this is treating AIDL nullability vs out-ptrs correctly; it may not be
ndk::ScopedAStatus TestAidlMsgQ::getFmqAidlTypesUnsyncWrite(
        bool in_configureFmq, bool in_userFd,
        std::optional<MQDescriptor<ITestAidlMsgQ::StructPayload, UnsynchronizedWrite>>*
                out_mqDescStruct,
        std::optional<MQDescriptor<ITestAidlMsgQ::UnionPayload, UnsynchronizedWrite>>*
                out_mqDescUnion,
        std::optional<MQDescriptor<ITestAidlMsgQ::EnumPayload, UnsynchronizedWrite>>*
                out_mqDescEnum,
        bool* _aidl_return) {
    /* enforce that exactly one argument is non-empty. */
    int n_nonempty = out_mqDescStruct->has_value() + out_mqDescUnion->has_value() +
                     out_mqDescEnum->has_value();
    if (n_nonempty != 1) {
        return ndk::ScopedAStatus::fromExceptionCode(EX_ILLEGAL_ARGUMENT);
    }

    if (out_mqDescStruct->has_value()) {
        *_aidl_return = getFmqUnsyncWriteGeneric(
                in_configureFmq, in_userFd, &out_mqDescStruct->value(), mFmqUnsynchronizedStruct);
    }
    if (out_mqDescUnion->has_value()) {
        *_aidl_return = getFmqUnsyncWriteGeneric(
                in_configureFmq, in_userFd, &out_mqDescUnion->value(), mFmqUnsynchronizedUnion);
    }
    if (out_mqDescEnum->has_value()) {
        *_aidl_return = getFmqUnsyncWriteGeneric(in_configureFmq, in_userFd,
                                                 &out_mqDescEnum->value(), mFmqUnsynchronizedEnum);
    }
    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestBlockingRead(int32_t count) {
    bool result;
    if (mFmqSynchronized != nullptr) {
        std::vector<int32_t> data(count);
        result = mFmqSynchronized->readBlocking(
                &data[0], count, static_cast<uint32_t>(EventFlagBits::FMQ_NOT_FULL),
                static_cast<uint32_t>(EventFlagBits::FMQ_NOT_EMPTY), 5000000000 /* timeOutNanos */);
    }
    if (mFmqSynchronizedStruct != nullptr) {
        std::vector<StructPayload> data(count);
        result = mFmqSynchronizedStruct->readBlocking(
                &data[0], count, static_cast<uint32_t>(EventFlagBits::FMQ_NOT_FULL),
                static_cast<uint32_t>(EventFlagBits::FMQ_NOT_EMPTY), 5000000000 /* timeOutNanos */);
    }
    if (mFmqSynchronizedEnum != nullptr) {
        std::vector<EnumPayload> data(count);
        result = mFmqSynchronizedEnum->readBlocking(
                &data[0], count, static_cast<uint32_t>(EventFlagBits::FMQ_NOT_FULL),
                static_cast<uint32_t>(EventFlagBits::FMQ_NOT_EMPTY), 5000000000 /* timeOutNanos */);
    }
    if (mFmqSynchronizedUnion != nullptr) {
        std::vector<UnionPayload> data(count);
        result = mFmqSynchronizedUnion->readBlocking(
                &data[0], count, static_cast<uint32_t>(EventFlagBits::FMQ_NOT_FULL),
                static_cast<uint32_t>(EventFlagBits::FMQ_NOT_EMPTY), 5000000000 /* timeOutNanos */);
    }

    if (result == false) {
        ALOGE("Blocking read fails");
    }
    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestBlockingReadDefaultEventFlagBits(int32_t count) {
    std::vector<int32_t> data(count);
    bool result;
    if (mFmqSynchronized != nullptr) {
        std::vector<int32_t> data(count);
        result = mFmqSynchronized->readBlocking(&data[0], count);
    }
    if (mFmqSynchronizedStruct != nullptr) {
        std::vector<StructPayload> data(count);
        result = mFmqSynchronizedStruct->readBlocking(&data[0], count);
    }
    if (mFmqSynchronizedEnum != nullptr) {
        std::vector<EnumPayload> data(count);
        result = mFmqSynchronizedEnum->readBlocking(&data[0], count);
    }
    if (mFmqSynchronizedUnion != nullptr) {
        std::vector<UnionPayload> data(count);
        result = mFmqSynchronizedUnion->readBlocking(&data[0], count);
    }

    if (result == false) {
        ALOGE("Blocking read fails");
    }

    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestBlockingReadRepeat(int32_t count, int32_t numIter) {
    for (int i = 0; i < numIter; i++) {
        requestBlockingRead(count);
    }

    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestReadFmqSync(int32_t count, bool* _aidl_return) {
    // TODO: port verifyData to other types
    bool result;
    if (mFmqSynchronized != nullptr) {
        std::vector<int32_t> data(count);
        result = mFmqSynchronized->read(&data[0], count) && verifyData(&data[0], count);
    }
    if (mFmqSynchronizedStruct != nullptr) {
        std::vector<StructPayload> data(count);
        result = mFmqSynchronizedStruct->read(&data[0], count);
    }
    if (mFmqSynchronizedEnum != nullptr) {
        std::vector<EnumPayload> data(count);
        result = mFmqSynchronizedEnum->read(&data[0], count);
    }
    if (mFmqSynchronizedUnion != nullptr) {
        std::vector<UnionPayload> data(count);
        result = mFmqSynchronizedUnion->read(&data[0], count);
    }

    *_aidl_return = result;
    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestReadFmqUnsync(int32_t count, bool* _aidl_return) {
    // TODO: port verifyData to other types
    bool result;
    if (mFmqUnsynchronized != nullptr) {
        std::vector<int32_t> data(count);
        result = mFmqUnsynchronized->read(&data[0], count) && verifyData(&data[0], count);
    }
    if (mFmqUnsynchronizedStruct != nullptr) {
        std::vector<StructPayload> data(count);
        result = mFmqUnsynchronizedStruct->read(&data[0], count);
    }
    if (mFmqUnsynchronizedEnum != nullptr) {
        std::vector<EnumPayload> data(count);
        result = mFmqUnsynchronizedEnum->read(&data[0], count);
    }
    if (mFmqUnsynchronizedUnion != nullptr) {
        std::vector<UnionPayload> data(count);
        result = mFmqUnsynchronizedUnion->read(&data[0], count);
    }

    *_aidl_return = result;
    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestWriteFmqSync(int32_t count, bool* _aidl_return) {
    // TODO: port to other types
    std::vector<int32_t> data(count);
    for (int i = 0; i < count; i++) {
        data[i] = i;
    }
    bool result = mFmqSynchronized->write(&data[0], count);
    *_aidl_return = result;
    return ndk::ScopedAStatus::ok();
}

ndk::ScopedAStatus TestAidlMsgQ::requestWriteFmqUnsync(int32_t count, bool* _aidl_return) {
    // TODO: port to other types
    std::vector<int32_t> data(count);
    for (int i = 0; i < count; i++) {
        data[i] = i;
    }
    if (!mFmqUnsynchronized) {
        ALOGE("Unsynchronized queue is not configured.");
        *_aidl_return = false;
        return ndk::ScopedAStatus::ok();
    }
    bool result = mFmqUnsynchronized->write(&data[0], count);
    *_aidl_return = result;
    return ndk::ScopedAStatus::ok();
}

}  // namespace test
}  // namespace fmq
}  // namespace android
}  // namespace aidl
