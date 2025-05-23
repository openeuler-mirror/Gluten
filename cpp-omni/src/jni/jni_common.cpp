/**
 * Copyright (C) 2022-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#ifndef THESTRAL_PLUGIN_MASTER_JNI_COMMON_CPP
#define THESTRAL_PLUGIN_MASTER_JNI_COMMON_CPP

#include "jni_common.h"
#include "io/SparkFile.hh"
#include "SparkJniWrapper.hh"

jclass runtimeExceptionClass;
jclass splitResultClass;
jclass jsonClass;
jclass arrayListClass;
jclass threadClass;
jclass serializedColumnarBatchIteratorClass;
jclass vecBatchCls;
jclass infoCls;
jclass runtimeAwareClass;

jmethodID jsonMethodInt;
jmethodID jsonMethodLong;
jmethodID jsonMethodHas;
jmethodID jsonMethodString;
jmethodID jsonMethodJsonObj;
jmethodID arrayListGet;
jmethodID arrayListSize;
jmethodID jsonMethodObj;
jmethodID splitResultConstructor;
jmethodID currentThread;
jmethodID threadGetId;
jmethodID serializedColumnarBatchIteratorHasNext;
jmethodID serializedColumnarBatchIteratorNext;
jmethodID vecBatchInitMethodId;
jmethodID method;
jmethodID runtimeAwareCtxHandle;

static jint JNI_VERSION = JNI_VERSION_1_8;

JniColumnarBatchIterator::JniColumnarBatchIterator(JNIEnv *env, jobject jColumnarBatchItr)
{
    // IMPORTANT: DO NOT USE LOCAL REF IN DIFFERENT THREAD
    if (env->GetJavaVM(&vm_) != JNI_OK) {
        std::string errorMessage = "Unable to get JavaVM instance";
        throw std::runtime_error(errorMessage);
    }
    serializedColumnarBatchIteratorClass_ =
        createGlobalClassReferenceOrError(env, "Lorg/apache/gluten/vectorized/OmniColumnarBatchInIterator;");
    serializedColumnarBatchIteratorHasNext_ =
        getMethodIdOrError(env, serializedColumnarBatchIteratorClass_, "hasNext", "()Z");
    serializedColumnarBatchIteratorNext_ =
        getMethodIdOrError(env, serializedColumnarBatchIteratorClass_, "next", "()J");
    jColumnarBatchItr_ = env->NewGlobalRef(jColumnarBatchItr);
}

JniColumnarBatchIterator::~JniColumnarBatchIterator()
{
    JNIEnv *env = nullptr;
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    env->DeleteGlobalRef(jColumnarBatchItr_);
    env->DeleteGlobalRef(serializedColumnarBatchIteratorClass_);
    vm_->DetachCurrentThread();
}

VectorBatch *JniColumnarBatchIterator::Next()
{
    JNIEnv *env = nullptr;
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
    if (!env->CallBooleanMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorHasNext_)) {
        CheckException(env);
        return nullptr;
    }
    CheckException(env);
    jlong handle = env->CallLongMethod(jColumnarBatchItr_, serializedColumnarBatchIteratorNext_);
    CheckException(env);
    return reinterpret_cast<VectorBatch *>(handle);
}

spark::CompressionKind GetCompressionType(JNIEnv* env, jstring codec_jstr)
{
    auto codec_c = env->GetStringUTFChars(codec_jstr, JNI_FALSE);
    auto codec = std::string(codec_c);
    auto compression_type = GetCompressionType(codec);
    env->ReleaseStringUTFChars(codec_jstr, codec_c);
    return compression_type;
}

jclass CreateGlobalClassReference(JNIEnv* env, const char* class_name)
{
    jclass local_class = env->FindClass(class_name);
    jclass global_class = (jclass)env->NewGlobalRef(local_class);
    env->DeleteLocalRef(local_class);
    return global_class;
}

jmethodID GetMethodID(JNIEnv* env, jclass this_class, const char* name, const char* sig)
{
    jmethodID ret = env->GetMethodID(this_class, name, sig);
    return ret;
}

jint JNI_OnLoad(JavaVM* vm, void* reserved)
{
    JNIEnv* env;
    if (vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION) != JNI_OK) {
        return JNI_ERR;
    }

    runtimeExceptionClass = CreateGlobalClassReference(env, "Ljava/lang/RuntimeException;");

    splitResultClass =
        CreateGlobalClassReference(env, "Lcom/huawei/boostkit/spark/vectorized/SplitResult;");
    splitResultConstructor = GetMethodID(env, splitResultClass, "<init>", "(JJJJJ[J)V");

    jsonClass = CreateGlobalClassReference(env, "org/json/JSONObject");
    jsonMethodInt = env->GetMethodID(jsonClass, "getInt", "(Ljava/lang/String;)I");
    jsonMethodLong = env->GetMethodID(jsonClass, "getLong", "(Ljava/lang/String;)J");
    jsonMethodHas = env->GetMethodID(jsonClass, "has", "(Ljava/lang/String;)Z");
    jsonMethodString = env->GetMethodID(jsonClass, "getString", "(Ljava/lang/String;)Ljava/lang/String;");
    jsonMethodJsonObj = env->GetMethodID(jsonClass, "getJSONObject", "(Ljava/lang/String;)Lorg/json/JSONObject;");
    jsonMethodObj = env->GetMethodID(jsonClass, "get", "(Ljava/lang/String;)Ljava/lang/Object;");

    arrayListClass = CreateGlobalClassReference(env, "java/util/ArrayList");
    arrayListGet = env->GetMethodID(arrayListClass, "get", "(I)Ljava/lang/Object;");
    arrayListSize = env->GetMethodID(arrayListClass, "size", "()I");

    threadClass = CreateGlobalClassReference(env, "java/lang/Thread");
    currentThread = env->GetStaticMethodID(threadClass, "currentThread", "()Ljava/lang/Thread;");
    threadGetId = env->GetMethodID(threadClass, "getId", "()J");

    vecBatchCls = CreateGlobalClassReference(env, "nova/hetu/omniruntime/vector/VecBatch");
    vecBatchInitMethodId = env->GetMethodID(vecBatchCls, "<init>", "(J[J[J[J[J[I[II)V");

    runtimeAwareClass = CreateGlobalClassReference(env, "Lorg/apache/gluten/runtime/RuntimeAware;");
    runtimeAwareCtxHandle = getMethodIdOrError(env, runtimeAwareClass, "rtHandle", "()J");
    infoCls = env->FindClass("Lorg/apache/gluten/validate/NativePlanValidationInfo;");
    if (infoCls == nullptr) {
        std::string errorMessage = "Unable to CreateGlobalClassReferenceOrError for NativePlanValidationInfo";
        OMNI_THROW("RUNTIME_ERROR", errorMessage);
    }
    method = env->GetMethodID(infoCls, "<init>", "(ILjava/lang/String;)V");

    return JNI_VERSION;
}

void JNI_OnUnload(JavaVM* vm, void* reserved)
{
    JNIEnv* env;
    vm->GetEnv(reinterpret_cast<void**>(&env), JNI_VERSION);

    env->DeleteGlobalRef(runtimeExceptionClass);
    env->DeleteGlobalRef(splitResultClass);
    env->DeleteGlobalRef(jsonClass);
    env->DeleteGlobalRef(arrayListClass);
    env->DeleteGlobalRef(threadClass);
}

omniruntime::Runtime *GetRuntime(JNIEnv *env, jobject runtimeAware)
{
    int64_t ctxHandle = env->CallLongMethod(runtimeAware, runtimeAwareCtxHandle);
    CheckException(env);
    auto ctx = reinterpret_cast<omniruntime::Runtime*>(ctxHandle);
    OMNI_CHECK(ctx != nullptr, "FATAL: resource instance should not be null.");
    return ctx;
}

#endif //THESTRAL_PLUGIN_MASTER_JNI_COMMON_CPP
