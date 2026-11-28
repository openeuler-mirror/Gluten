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
/*
 * JniGlobalRef RAII header.
 *
 * RAII owner for a JNI global reference (jobject).
 *   - Construction: requires an attached JNIEnv; calls env->NewGlobalRef.
 *   - Destruction:  tolerates detached state (JVM shutdown / JIT codegen thread).
 *                   Calls env->DeleteGlobalRef if vm->GetEnv succeeds; else accepts
 *                   the leak (the JVM is tearing down anyway).
 *   - Move-only — copies would double-delete the same global ref.
 *
 * Lets OmniSparkAllocationListener hold its Java listener without a bare jobject.
 * RAII keeps NewGlobalRef/DeleteGlobalRef symmetric even under C++ exception
 * unwind, so the global-ref table cannot be exhausted by leaked references.
 */
#pragma once

#include <jni.h>

namespace gluten {

class JniGlobalRef {
 public:
    JniGlobalRef() = default;                                    // null-state for field default-init

    JniGlobalRef(JavaVM* vm, JNIEnv* env, jobject localRef)
        : vm_(vm), ref_(localRef ? env->NewGlobalRef(localRef) : nullptr) {}

    ~JniGlobalRef() noexcept { release(); }

    // Null both vm_ and ref_ on the moved-from object so it is left in
    // a fully-consistent null state. Nulling only ref_ left a stale non-null
    // vm_ behind — harmless for release() today (it short-circuits on ref_ ==
    // nullptr) but a latent trap for any future reset()/swap() or code that
    // consults vm_ on a moved-from object.
    JniGlobalRef(JniGlobalRef&& other) noexcept
        : vm_(other.vm_), ref_(other.ref_) {
        other.vm_ = nullptr;
        other.ref_ = nullptr;
    }
    JniGlobalRef& operator=(JniGlobalRef&& other) noexcept {
        if (this != &other) {
            release();
            vm_ = other.vm_;
            ref_ = other.ref_;
            other.vm_ = nullptr;
            other.ref_ = nullptr;
        }
        return *this;
    }
    JniGlobalRef(const JniGlobalRef&) = delete;
    JniGlobalRef& operator=(const JniGlobalRef&) = delete;

    jobject get() const noexcept { return ref_; }
    explicit operator bool() const noexcept { return ref_ != nullptr; }

 private:
    void release() noexcept {
        if (ref_ == nullptr || vm_ == nullptr) return;
        JNIEnv* env = nullptr;
        // Mirror JniCommon.h:386-388 — tolerate detached JVM-shutdown / JIT threads.
        // vm_->GetEnv returns non-JNI_OK when called from a non-attached thread
        // (e.g., JIT codegen thread or during JVM teardown). Accept the leak; the
        // JVM is going down anyway.
        if (vm_->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION_1_8) != JNI_OK) {
            ref_ = nullptr;   // accept leak — JVM tearing down
            return;
        }
        env->DeleteGlobalRef(ref_);
        ref_ = nullptr;
    }
    JavaVM* vm_ = nullptr;
    jobject ref_ = nullptr;
};

} // namespace gluten
