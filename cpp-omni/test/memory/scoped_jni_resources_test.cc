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
#include "gtest/gtest.h"
#include <unistd.h>
#include "jni/ScopedJniResources.h"

namespace {
class ScopedJniResourcesTest : public ::testing::Test {
protected:
    inline static int releases, attaches, detaches, localDeletes, throws;
    inline static bool pending;
    inline static jint envStatus, attachStatus;
    inline static bool failAcquire;
    inline static jbyte bytes[2] = {1, 2};
    inline static JNIEnv env;
    JNINativeInterface_ native{};
    JNIInvokeInterface_ invoke{};
    JavaVM vm{};
    jbyteArray array = reinterpret_cast<jbyteArray>(bytes);

    void SetUp() override
    {
        releases = attaches = detaches = localDeletes = throws = 0;
        pending = false;
        native.ExceptionCheck = [](JNIEnv *) -> jboolean { return pending; };
        native.ThrowNew = [](JNIEnv *, jclass, const char *) -> jint {
            ++throws;
            pending = true;
            return JNI_OK;
        };
        native.DeleteLocalRef = [](JNIEnv *, jobject ref) {
            EXPECT_NE(ref, nullptr);
            ++localDeletes;
        };
        envStatus = attachStatus = JNI_OK;
        failAcquire = false;
        native.GetByteArrayElements = [](JNIEnv *, jbyteArray, jboolean *) -> jbyte * {
            return failAcquire ? nullptr : bytes;
        };
        native.ReleaseByteArrayElements = [](JNIEnv *, jbyteArray, jbyte *p, jint mode) {
            EXPECT_EQ(p, bytes);
            EXPECT_EQ(mode, JNI_ABORT);
            ++releases;
        };
        env.functions = &native;
        invoke.GetEnv = [](JavaVM *, void **out, jint) -> jint {
            *out = envStatus == JNI_OK ? &env : nullptr;
            return envStatus;
        };
        invoke.AttachCurrentThreadAsDaemon = [](JavaVM *, void **out, void *) -> jint {
            ++attaches;
            *out = attachStatus == JNI_OK ? &env : nullptr;
            return attachStatus;
        };
        invoke.DetachCurrentThread = [](JavaVM *) -> jint { ++detaches; return JNI_OK; };
        vm.functions = &invoke;
    }
};

TEST_F(ScopedJniResourcesTest, ExceptionTranslationPreservesPendingJavaFailure)
{
    pending = true;
    gluten::ThrowJavaExceptionIfNone(&env, nullptr, "native failure");
    EXPECT_EQ(throws, 0);
    EXPECT_TRUE(pending);
    pending = false;
    gluten::ThrowJavaExceptionIfNone(&env, nullptr, "native failure");
    EXPECT_EQ(throws, 1);
    EXPECT_TRUE(pending);
}

TEST_F(ScopedJniResourcesTest, LocalReferenceReleasesOnUnwindAndIgnoresNull)
{
    { gluten::ScopedLocalRef<jobject> ref(&env, nullptr); }
    EXPECT_EQ(localDeletes, 0);
    EXPECT_THROW({
        gluten::ScopedLocalRef<jbyteArray> ref(&env, array);
        EXPECT_EQ(ref.get(), array);
        throw std::runtime_error("iterator construction failed");
    }, std::runtime_error);
    EXPECT_EQ(localDeletes, 1);
}

TEST_F(ScopedJniResourcesTest, ByteArrayReleasesOnNormalExit)
{
    { gluten::ScopedByteArrayElements scope(&env, array); EXPECT_EQ(scope.data()[1], 2); }
    EXPECT_EQ(releases, 1);
}

TEST_F(ScopedJniResourcesTest, ByteArrayReleasesOnException)
{
    EXPECT_THROW({
        gluten::ScopedByteArrayElements scope(&env, array);
        throw std::runtime_error("parse failed");
    }, std::runtime_error);
    EXPECT_EQ(releases, 1);
}

TEST_F(ScopedJniResourcesTest, FailedAcquireDoesNotRelease)
{
    failAcquire = true;
    EXPECT_THROW(gluten::ScopedByteArrayElements scope(&env, array), std::runtime_error);
    EXPECT_EQ(releases, 0);
}

TEST_F(ScopedJniResourcesTest, AlreadyAttachedThreadIsNotDetached)
{
    { gluten::ScopedJniEnv scope(&vm); EXPECT_EQ(scope.get(), &env); }
    EXPECT_EQ(attaches, 0);
    EXPECT_EQ(detaches, 0);
}

TEST_F(ScopedJniResourcesTest, OwnAttachmentIsDetached)
{
    envStatus = JNI_EDETACHED;
    { gluten::ScopedJniEnv scope(&vm); EXPECT_EQ(scope.get(), &env); }
    EXPECT_EQ(attaches, 1);
    EXPECT_EQ(detaches, 1);
}

TEST_F(ScopedJniResourcesTest, FailedAttachmentIsNonThrowing)
{
    envStatus = JNI_EDETACHED;
    attachStatus = JNI_ERR;
    { gluten::ScopedJniEnv scope(&vm); EXPECT_EQ(scope.get(), nullptr); }
    EXPECT_EQ(attaches, 1);
    EXPECT_EQ(detaches, 0);
}

TEST_F(ScopedJniResourcesTest, VersionErrorDoesNotAttachOrDetach)
{
    envStatus = JNI_EVERSION;
    { gluten::ScopedJniEnv scope(&vm); EXPECT_EQ(scope.get(), nullptr); }
    EXPECT_EQ(attaches, 0);
    EXPECT_EQ(detaches, 0);
}
} // namespace

extern "C" jlong Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_create(
    JNIEnv*, jclass, jstring, jobject, jbyteArray, jlong);

TEST(ReservationListenerInitDeathTest, RepeatedFailureAlwaysLeavesJavaException)
{
    ::testing::FLAGS_gtest_death_test_style = "threadsafe";
    // Keep call_once failure state in the child process, away from other tests.
    EXPECT_EXIT({
        static bool pending = false;
        static int lookups = 0;
        static int throws = 0;
        JNINativeInterface_ functions{};
        functions.FindClass = [](JNIEnv*, const char*) -> jclass {
            ++lookups;
            pending = true;
            return nullptr;
        };
        functions.ExceptionCheck = [](JNIEnv*) -> jboolean { return pending; };
        functions.ThrowNew = [](JNIEnv*, jclass, const char*) -> jint {
            ++throws;
            pending = true;
            return JNI_OK;
        };
        JNIEnv env{};
        env.functions = &functions;
        auto create = [&] {
            return Java_org_apache_gluten_memory_OmniNativeMemoryManagerJniWrapper_create(
                &env, nullptr, nullptr, nullptr, nullptr, 0);
        };
        if (create() != 0 || !pending || lookups != 1 || throws != 0) _exit(1);
        pending = false; // Java caller handled the first lookup exception.
        if (create() != 0 || !pending || lookups != 1 || throws != 1) _exit(2);
        _exit(0);
    }, ::testing::ExitedWithCode(0), "");
}
