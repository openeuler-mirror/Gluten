/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <jni.h>
#include <cstdint>
#include <cstdio>
#include <stdexcept>

namespace gluten {

inline void ThrowJavaExceptionIfNone(JNIEnv *env, jclass type, const char *message) noexcept
{
    if (!env->ExceptionCheck()) {
        env->ThrowNew(type, message);
    }
}

// Own one local reference, including while native construction unwinds.
template <typename T> class ScopedLocalRef final {
public:
    ScopedLocalRef(JNIEnv *env, T ref) noexcept : env_(env), ref_(ref) {}
    ~ScopedLocalRef() noexcept { if (ref_ != nullptr) env_->DeleteLocalRef(ref_); }
    ScopedLocalRef(const ScopedLocalRef &) = delete;
    ScopedLocalRef &operator=(const ScopedLocalRef &) = delete;
    T get() const noexcept { return ref_; }
private:
    JNIEnv *env_;
    T ref_;
};

// Read-only byte-array access. Release on normal return AND C++ unwinding.
class ScopedByteArrayElements final {
public:
    ScopedByteArrayElements(JNIEnv *env, jbyteArray array) : env_(env), array_(array)
    {
        if (array_ == nullptr) {
            throw std::invalid_argument("Null JNI byte array");
        }
        bytes_ = env_->GetByteArrayElements(array_, nullptr);
        if (bytes_ == nullptr) {
            throw std::runtime_error("Unable to acquire JNI byte array");
        }
    }
    ~ScopedByteArrayElements() noexcept
    {
        env_->ReleaseByteArrayElements(array_, bytes_, JNI_ABORT);
    }
    ScopedByteArrayElements(const ScopedByteArrayElements &) = delete;
    ScopedByteArrayElements &operator=(const ScopedByteArrayElements &) = delete;
    const uint8_t *data() const { return reinterpret_cast<const uint8_t *>(bytes_); }

private:
    JNIEnv *env_;
    jbyteArray array_;
    jbyte *bytes_ = nullptr;
};

// Destructor-safe JNI access: never detach a thread attached by somebody else.
class ScopedJniEnv final {
public:
    explicit ScopedJniEnv(JavaVM *vm) noexcept : vm_(vm)
    {
        const auto status = vm_->GetEnv(reinterpret_cast<void **>(&env_), JNI_VERSION_1_8);
        if (status == JNI_EDETACHED) {
            attached_ = vm_->AttachCurrentThreadAsDaemon(reinterpret_cast<void **>(&env_), nullptr) == JNI_OK;
            if (!attached_) env_ = nullptr;
        } else if (status != JNI_OK) {
            env_ = nullptr;
        }
    }
    ~ScopedJniEnv() noexcept
    {
        if (attached_ && vm_->DetachCurrentThread() != JNI_OK) {
            std::fprintf(stderr, "[OMNI-JNI] Failed to detach cleanup thread\n");
        }
    }
    ScopedJniEnv(const ScopedJniEnv &) = delete;
    ScopedJniEnv &operator=(const ScopedJniEnv &) = delete;
    JNIEnv *get() const { return env_; }

private:
    JavaVM *vm_;
    JNIEnv *env_ = nullptr;
    bool attached_ = false;
};

} // namespace gluten
