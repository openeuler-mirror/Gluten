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
#include <jni.h>

#include <atomic>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "memory/AllocationListener.h"
#include "memory/reservation_denied.h"
#include "../compute/OmniBackend.h"

namespace {

class AttachedEnv {
public:
    explicit AttachedEnv(JavaVM* vm) : vm_(vm)
    {
        const jint status = vm_->GetEnv(reinterpret_cast<void**>(&env_), JNI_VERSION_1_8);
        if (status == JNI_EDETACHED) {
            if (vm_->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(&env_), nullptr) != JNI_OK) {
                throw std::runtime_error("Cannot attach broadcast reservation thread to JVM");
            }
            attached_ = true;
        } else if (status != JNI_OK) {
            throw std::runtime_error("Cannot obtain JVM for broadcast reservation");
        }
    }

    ~AttachedEnv()
    {
        if (attached_) {
            vm_->DetachCurrentThread();
        }
    }

    JNIEnv* get() const { return env_; }

private:
    JavaVM* vm_;
    JNIEnv* env_{nullptr};
    bool attached_{false};
};

class OmniGlobalSparkAllocationListener final : public gluten::AllocationListener {
public:
    OmniGlobalSparkAllocationListener(JNIEnv* env, jobject listener)
    {
        if (listener == nullptr || env->GetJavaVM(&vm_) != JNI_OK) {
            throw std::invalid_argument("Broadcast reservation listener or JVM is absent");
        }
        jclass listenerClass = env->GetObjectClass(listener);
        if (listenerClass == nullptr) {
            env->ExceptionClear();
            throw std::runtime_error("Cannot resolve broadcast listener class");
        }
        reserveMethod_ = env->GetMethodID(listenerClass, "reserve", "(J)J");
        unreserveMethod_ = env->GetMethodID(listenerClass, "unreserve", "(J)J");
        env->DeleteLocalRef(listenerClass);
        if (reserveMethod_ == nullptr || unreserveMethod_ == nullptr || env->ExceptionCheck()) {
            env->ExceptionClear();
            throw std::runtime_error("Cannot resolve broadcast reservation methods");
        }
        listener_ = env->NewGlobalRef(listener);
        if (listener_ == nullptr || env->ExceptionCheck()) {
            env->ExceptionClear();
            throw std::runtime_error("Cannot retain broadcast reservation listener");
        }
    }

    ~OmniGlobalSparkAllocationListener() override
    {
        if (listener_ == nullptr) {
            return;
        }
        try {
            AttachedEnv attached(vm_);
            attached.get()->DeleteGlobalRef(listener_);
        } catch (...) {
            // JVM teardown can prevent attachment. The process is exiting.
        }
    }

    void allocationChanged(int64_t delta) override
    {
        if (delta == 0) {
            return;
        }
        std::lock_guard<std::mutex> lock(mutex_);
        if (delta < 0 && -delta > used_) {
            throw std::logic_error("Broadcast cache repayment exceeds granted bytes");
        }
        AttachedEnv attached(vm_);
        JNIEnv* env = attached.get();
        const bool positive = delta > 0;
        const jlong requested = positive ? delta : -delta;
        const jlong granted = env->CallLongMethod(
            listener_, positive ? reserveMethod_ : unreserveMethod_, requested);
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
            throw std::runtime_error("Java broadcast reservation callback failed");
        }
        if (positive && granted == 0) {
            throw omniruntime::mem::ReservationDenied("Spark denied broadcast cache storage");
        }
        if (granted != requested) {
            throw std::runtime_error("Broadcast reservation callback returned a partial amount");
        }
        used_ += delta;
        if (used_ > peak_) {
            peak_ = used_;
        }
    }

    int64_t currentBytes() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return used_;
    }

    int64_t peakBytes() override
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return peak_;
    }

private:
    JavaVM* vm_{nullptr};
    jobject listener_{nullptr};
    jmethodID reserveMethod_{nullptr};
    jmethodID unreserveMethod_{nullptr};
    std::mutex mutex_;
    int64_t used_{0};
    int64_t peak_{0};
};

} // namespace

extern "C" JNIEXPORT void JNICALL
Java_org_apache_gluten_init_OmniNativeBackendInitializer_initializeBroadcastCache0(
    JNIEnv* env, jobject, jobject listener)
{
    try {
        gluten::OmniBackend::configureBroadcastCache(
            std::make_unique<OmniGlobalSparkAllocationListener>(env, listener));
    } catch (const std::exception& error) {
        if (env->ExceptionCheck()) {
            env->ExceptionClear();
        }
        jclass exceptionClass = env->FindClass("java/lang/IllegalStateException");
        if (exceptionClass != nullptr) {
            env->ThrowNew(exceptionClass, error.what());
            env->DeleteLocalRef(exceptionClass);
        }
    }
}

extern "C" JNIEXPORT void JNICALL
Java_org_apache_gluten_init_OmniNativeBackendInitializer_shutdownBroadcastCache0(
    JNIEnv* env, jobject)
{
    try {
        gluten::OmniBackend::shutdownBroadcastCache();
    } catch (const std::exception& error) {
        jclass exceptionClass = env->FindClass("java/lang/IllegalStateException");
        if (exceptionClass != nullptr) {
            env->ThrowNew(exceptionClass, error.what());
            env->DeleteLocalRef(exceptionClass);
        }
    }
}
