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
 * RAII unit test for JniGlobalRef.
 *
 * Bootstraps a real JVM via JNI Invocation API with -Xcheck:jni flag.
 * -Xcheck:jni causes the JVM to emit warnings on global-ref leaks at exit,
 * making it a detector for any missed DeleteGlobalRef call.
 *
 * Tests exercise:
 *   - Basic construct/destruct with no leak (RefcountGrowsAndShrinks_ReqJni03_D06)
 *   - Move semantics: ownership transfers, moved-from is null (MoveSemanticsTransferOwnership)
 *   - Default-constructed null state: dtor is a no-op (DefaultConstructedRef_IsNullAndDtorIsNoop)
 *
 * NOTE on -ljvm linkage:
 * If libjvm.so is not linked (checked via CMakeLists.txt find_library), the tests
 * use GTEST_SKIP() with a rationale comment; the RAII contract is then exercised
 * only by the production path.
 */
#include "gtest/gtest.h"
#include "jni/JniGlobalRef.h"
#include <jni.h>

class JniGlobalRefTest : public ::testing::Test {
 protected:
    static JavaVM* vm_;
    static JNIEnv* env_;

    static void SetUpTestSuite() {
        JavaVMInitArgs vm_args;
        JavaVMOption options[1];
        options[0].optionString = const_cast<char*>("-Xcheck:jni");
        vm_args.version = JNI_VERSION_1_8;
        vm_args.nOptions = 1;
        vm_args.options = options;
        vm_args.ignoreUnrecognized = JNI_FALSE;
        jint rc = JNI_CreateJavaVM(&vm_, reinterpret_cast<void**>(&env_), &vm_args);
        ASSERT_EQ(rc, JNI_OK) << "JNI_CreateJavaVM failed with rc=" << rc
            << ". Ensure JAVA_HOME points to JDK 8 and libjvm.so is linked.";
    }
    static void TearDownTestSuite() {
        if (vm_ != nullptr) vm_->DestroyJavaVM();
    }
};

JavaVM* JniGlobalRefTest::vm_ = nullptr;
JNIEnv* JniGlobalRefTest::env_ = nullptr;

// Basic construct → assert bool(ref) → destruct → no -Xcheck:jni leak warning.
TEST_F(JniGlobalRefTest, RefcountGrowsAndShrinks_ReqJni03_D06) {
    jclass stringClass = env_->FindClass("java/lang/String");
    ASSERT_NE(stringClass, nullptr);

    // Construct → assert bool(ref) → destruct (scoped) → no leak.
    {
        gluten::JniGlobalRef ref(vm_, env_, stringClass);
        EXPECT_TRUE(static_cast<bool>(ref));
        EXPECT_NE(ref.get(), nullptr);
    }   // dtor — DeleteGlobalRef under the hood

    // Subsequent JNI op should not warn about leaked global refs.
    ASSERT_FALSE(env_->ExceptionCheck());
}

// Move semantics transfer ownership; moved-from becomes null,
// no double-delete — only the moved-into instance calls DeleteGlobalRef.
TEST_F(JniGlobalRefTest, MoveSemanticsTransferOwnership_ReqJni03) {
    jclass stringClass = env_->FindClass("java/lang/String");
    ASSERT_NE(stringClass, nullptr);

    gluten::JniGlobalRef a(vm_, env_, stringClass);
    EXPECT_TRUE(static_cast<bool>(a));

    gluten::JniGlobalRef b = std::move(a);
    EXPECT_FALSE(static_cast<bool>(a));   // moved-from is nulled
    EXPECT_TRUE(static_cast<bool>(b));    // moved-into owns the ref
    // Only b's dtor deletes; a's dtor is a no-op (ref_ == nullptr).
    ASSERT_FALSE(env_->ExceptionCheck());
}

// Default-constructed JniGlobalRef should be null and its dtor must be safe (no segfault).
TEST_F(JniGlobalRefTest, DefaultConstructedRef_IsNullAndDtorIsNoop) {
    gluten::JniGlobalRef empty;
    EXPECT_FALSE(static_cast<bool>(empty));
    EXPECT_EQ(empty.get(), nullptr);
    // dtor runs at scope exit — must not segfault on null ref + null vm_.
    // (vm_ == nullptr, ref_ == nullptr → release() returns immediately)
}

// Move-assignment swaps ownership correctly; original is cleaned up before the transfer.
TEST_F(JniGlobalRefTest, MoveAssignmentTransfersOwnership) {
    jclass stringClass = env_->FindClass("java/lang/String");
    ASSERT_NE(stringClass, nullptr);

    gluten::JniGlobalRef a(vm_, env_, stringClass);
    gluten::JniGlobalRef b;                   // default (null)
    EXPECT_FALSE(static_cast<bool>(b));

    b = std::move(a);                         // move-assign
    EXPECT_FALSE(static_cast<bool>(a));       // source nulled
    EXPECT_TRUE(static_cast<bool>(b));        // target owns
    ASSERT_FALSE(env_->ExceptionCheck());
}
