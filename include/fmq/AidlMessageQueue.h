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

#include <aidl/android/hardware/common/MQDescriptor.h>
#include <aidl/android/hardware/common/SynchronizedReadWrite.h>
#include <aidl/android/hardware/common/UnsynchronizedWrite.h>
#include <cutils/native_handle.h>
#include <fmq/AidlMQDescriptorShim.h>
#include <fmq/MessageQueueBase.h>
#include <utils/Log.h>
#include <type_traits>

namespace android {

using aidl::android::hardware::common::MQDescriptor;
using aidl::android::hardware::common::SynchronizedReadWrite;
using aidl::android::hardware::common::UnsynchronizedWrite;
using android::details::AidlMQDescriptorShim;
using android::hardware::MQFlavor;

template <typename T>
struct FlavorTypeToValue;

template <>
struct FlavorTypeToValue<SynchronizedReadWrite> {
    static constexpr MQFlavor value = hardware::kSynchronizedReadWrite;
};

template <>
struct FlavorTypeToValue<UnsynchronizedWrite> {
    static constexpr MQFlavor value = hardware::kUnsynchronizedWrite;
};

typedef uint64_t RingBufferPosition;

/*
 * AIDL parcelables will have the typedef fixed_size. It is std::true_type when the
 * parcelable is annotated with @FixedSize, and std::false_type when not. Other types
 * should not have the fixed_size typedef, so they will always resolve to std::false_type.
 */
template <typename T, typename = void>
struct has_typedef_fixed_size : std::false_type {};

template <typename T>
struct has_typedef_fixed_size<T, std::void_t<typename T::fixed_size>> : T::fixed_size {};

template <typename T, typename U>
struct AidlMessageQueue final
    : public MessageQueueBase<AidlMQDescriptorShim, T, FlavorTypeToValue<U>::value> {
    static_assert(has_typedef_fixed_size<T>::value == true || std::is_fundamental<T>::value ||
                          std::is_enum<T>::value,
                  "Only fundamental types, enums, and AIDL parcelables annotated with @FixedSize "
                  "and built for the NDK backend are supported as payload types(T).");
    typedef AidlMQDescriptorShim<T, FlavorTypeToValue<U>::value> Descriptor;
    /**
     * This constructor uses the external descriptor used with AIDL interfaces.
     * It will create an FMQ based on the descriptor that was obtained from
     * another FMQ instance for communication.
     *
     * @param desc Descriptor from another FMQ that contains all of the
     * information required to create a new instance of that queue.
     * @param resetPointers Boolean indicating whether the read/write pointers
     * should be reset or not.
     */
    AidlMessageQueue(const MQDescriptor<T, U>& desc, bool resetPointers = true);
    ~AidlMessageQueue() = default;

    /**
     * This constructor uses Ashmem shared memory to create an FMQ
     * that can contain a maximum of 'numElementsInQueue' elements of type T.
     *
     * @param numElementsInQueue Capacity of the AidlMessageQueue in terms of T.
     * @param configureEventFlagWord Boolean that specifies if memory should
     * also be allocated and mapped for an EventFlag word.
     */
    AidlMessageQueue(size_t numElementsInQueue, bool configureEventFlagWord = false);
    MQDescriptor<T, U> dupeDesc();

  private:
    AidlMessageQueue(const AidlMessageQueue& other) = delete;
    AidlMessageQueue& operator=(const AidlMessageQueue& other) = delete;
    AidlMessageQueue() = delete;
};

template <typename T, typename U>
AidlMessageQueue<T, U>::AidlMessageQueue(const MQDescriptor<T, U>& desc, bool resetPointers)
    : MessageQueueBase<AidlMQDescriptorShim, T, FlavorTypeToValue<U>::value>(Descriptor(desc),
                                                                             resetPointers) {}

template <typename T, typename U>
AidlMessageQueue<T, U>::AidlMessageQueue(size_t numElementsInQueue, bool configureEventFlagWord)
    : MessageQueueBase<AidlMQDescriptorShim, T, FlavorTypeToValue<U>::value>(
              numElementsInQueue, configureEventFlagWord) {}

template <typename T, typename U>
MQDescriptor<T, U> AidlMessageQueue<T, U>::dupeDesc() {
    auto* shim = MessageQueueBase<AidlMQDescriptorShim, T, FlavorTypeToValue<U>::value>::getDesc();
    if (shim) {
        std::vector<aidl::android::hardware::common::GrantorDescriptor> grantors;
        for (const auto& grantor : shim->grantors()) {
            grantors.push_back(aidl::android::hardware::common::GrantorDescriptor{
                    .offset = static_cast<int32_t>(grantor.offset),
                    .extent = static_cast<int64_t>(grantor.extent)});
        }
        return MQDescriptor<T, U>{
                .quantum = static_cast<int32_t>(shim->getQuantum()),
                .grantors = grantors,
                .flags = static_cast<int32_t>(shim->getFlags()),
                .fileDescriptor = ndk::ScopedFileDescriptor(dup(shim->handle()->data[0])),
        };
    } else {
        return MQDescriptor<T, U>();
    }
}

}  // namespace android
