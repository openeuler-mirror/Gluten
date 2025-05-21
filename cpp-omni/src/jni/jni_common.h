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

#ifndef THESTRAL_PLUGIN_MASTER_JNI_COMMON_H
#define THESTRAL_PLUGIN_MASTER_JNI_COMMON_H

#include <jni.h>
#include "util/omni_exception.h"
#include "common/common.h"
#include "compute/ResultIterator.h"


class JniColumnarBatchIterator : public omniruntime::ColumnarBatchIterator {
public:
    explicit JniColumnarBatchIterator(JNIEnv *env, jobject jColumnarBatchItr);

    JniColumnarBatchIterator(const JniColumnarBatchIterator &) = delete;

    JniColumnarBatchIterator(JniColumnarBatchIterator &&) = delete;

    JniColumnarBatchIterator &operator=(const JniColumnarBatchIterator &) = delete;

    JniColumnarBatchIterator &operator=(JniColumnarBatchIterator &&) = delete;

    ~JniColumnarBatchIterator() override;

    omniruntime::vec::VectorBatch *Next() override;

private:
    JavaVM *vm_;
    jobject jColumnarBatchItr_;
    jclass serializedColumnarBatchIteratorClass_;
    jmethodID serializedColumnarBatchIteratorHasNext_;
    jmethodID serializedColumnarBatchIteratorNext_;
};

static jint jniVersion = JNI_VERSION_1_8;
static inline void AttachCurrentThreadAsDaemonOrThrow(JavaVM* vm, JNIEnv** out) {
    int getEnvStat = vm->GetEnv(reinterpret_cast<void**>(out), jniVersion);
    if (getEnvStat == JNI_EDETACHED) {
        // Reattach current thread to JVM
        getEnvStat = vm->AttachCurrentThreadAsDaemon(reinterpret_cast<void**>(out), NULL);
        if (getEnvStat != JNI_OK) {
            throw std::runtime_error("Failed to reattach current thread to JVM.");
        }
        return;
    }
    if (getEnvStat != JNI_OK) {
        throw std::runtime_error("Failed to attach current thread to JVM.");
    }
}

static inline void CheckException(JNIEnv* env) {
    if (env->ExceptionCheck()) {
        throw omniruntime::exception::OmniException("JNI_ERROR",
            "Error during calling Java code from native code.");
    }
}

static inline jclass createGlobalClassReference(JNIEnv *env, const char *className)
{
    jclass localClass = env->FindClass(className);
    jclass globalClass = (jclass)env->NewGlobalRef(localClass);
    env->DeleteLocalRef(localClass);
    return globalClass;
}

static inline jclass createGlobalClassReferenceOrError(JNIEnv *env, const char *className)
{
    jclass globalClass = createGlobalClassReference(env, className);
    if (globalClass == nullptr) {
        std::string errorMessage = "Unable to CreateGlobalClassReferenceOrError for" + std::string(className);
        throw std::runtime_error(errorMessage);
    }
    return globalClass;
}

static inline jmethodID getMethodId(JNIEnv *env, jclass thisClass, const char *name, const char *sig)
{
    jmethodID ret = env->GetMethodID(thisClass, name, sig);
    return ret;
}

static inline jmethodID getMethodIdOrError(JNIEnv *env, jclass thisClass, const char *name, const char *sig)
{
    jmethodID ret = getMethodId(env, thisClass, name, sig);
    if (ret == nullptr) {
        std::string errorMessage = "Unable to find method " + std::string(name) + " within signature" +
            std::string(sig);
        throw std::runtime_error(errorMessage);
    }
    return ret;
}

spark::CompressionKind GetCompressionType(JNIEnv* env, jstring codec_jstr);

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name);

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig);

#define JNI_FUNC_START try {

#define JNI_FUNC_END(exceptionClass)                \
    }                                               \
    catch (const std::exception &e)                 \
    {                                               \
        env->ThrowNew(exceptionClass, e.what());    \
        return 0;                                   \
    }                                               \


#define JNI_FUNC_END_VOID(exceptionClass)           \
    }                                               \
    catch (const std::exception &e)                 \
    {                                               \
        env->ThrowNew(exceptionClass, e.what());    \
        return;                                     \
    }                                               \

#define JNI_FUNC_END_WITH_VECBATCH(exceptionClass, toDeleteVecBatch) \
    }                                                                \
    catch (const std::exception &e)                                  \
    {                                                                \
        VectorHelper::FreeVecBatch(toDeleteVecBatch);                \
        env->ThrowNew(exceptionClass, e.what());          \
        return 0;                                         \
    }

#define JNI_FUNC_END_WITH_VECTORS(exceptionClass, vectors)       \
    } catch (const std::exception &e) {                          \
        for (auto vec : vectors) {                               \
            delete vec;                                          \
        }                                                        \
        env->ThrowNew(runtimeExceptionClass, e.what());          \
        return;                                                  \
    }                                                            \


extern jclass runtimeExceptionClass;
extern jclass splitResultClass;
extern jclass jsonClass;
extern jclass arrayListClass;
extern jclass threadClass;
extern jclass serializedColumnarBatchIteratorClass;
extern jclass vecBatchCls;
extern jclass infoCls;

extern jmethodID jsonMethodInt;
extern jmethodID jsonMethodLong;
extern jmethodID jsonMethodHas;
extern jmethodID jsonMethodString;
extern jmethodID jsonMethodJsonObj;
extern jmethodID arrayListGet;
extern jmethodID arrayListSize;
extern jmethodID jsonMethodObj;
extern jmethodID splitResultConstructor;
extern jmethodID currentThread;
extern jmethodID threadGetId;
extern jmethodID serializedColumnarBatchIteratorHasNext;
extern jmethodID serializedColumnarBatchIteratorNext;
extern jmethodID vecBatchInitMethodId;
extern jmethodID method;

#endif //THESTRAL_PLUGIN_MASTER_JNI_COMMON_H
