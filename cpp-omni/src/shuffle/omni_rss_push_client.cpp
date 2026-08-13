/**
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "shuffle/omni_rss_push_client.h"

#include "jni/jni_common.h"

OmniRssPushClient::OmniRssPushClient(JavaVM* vm, jobject partitionPusher, jmethodID pushPartitionDataMethod)
    : vm_(vm), pushPartitionData_(pushPartitionDataMethod)
{
    JNIEnv* env = nullptr;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        throw std::runtime_error("JNIEnv was not attached to current thread");
    }
    partitionPusher_ = env->NewGlobalRef(partitionPusher);
    byteArray_ = env->NewByteArray(1024 * 1024);
    byteArray_ = static_cast<jbyteArray>(env->NewGlobalRef(byteArray_));
}

OmniRssPushClient::~OmniRssPushClient()
{
    JNIEnv* env = nullptr;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        return;
    }
    env->DeleteGlobalRef(partitionPusher_);
    jbyte* nativeArray = env->GetByteArrayElements(byteArray_, nullptr);
    env->ReleaseByteArrayElements(byteArray_, nativeArray, JNI_ABORT);
    env->DeleteGlobalRef(byteArray_);
}

int32_t OmniRssPushClient::pushPartitionData(int32_t partitionId, const char* bytes, int64_t size)
{
    JNIEnv* env = nullptr;
    if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
        throw std::runtime_error("JNIEnv was not attached to current thread");
    }
    if (size <= 0) {
        return 0;
    }
    jint length = env->GetArrayLength(byteArray_);
    if (size > length) {
        jbyte* nativeArray = env->GetByteArrayElements(byteArray_, nullptr);
        env->ReleaseByteArrayElements(byteArray_, nativeArray, JNI_ABORT);
        env->DeleteGlobalRef(byteArray_);
        byteArray_ = env->NewByteArray(static_cast<jsize>(size));
        byteArray_ = static_cast<jbyteArray>(env->NewGlobalRef(byteArray_));
    }
    env->SetByteArrayRegion(byteArray_, 0, static_cast<jsize>(size), reinterpret_cast<const jbyte*>(bytes));
    jint pushed = env->CallIntMethod(partitionPusher_, pushPartitionData_, partitionId, byteArray_,
                                     static_cast<jint>(size));
    CheckException(env);
    return static_cast<int32_t>(pushed);
}
