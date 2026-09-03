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
#include "jni/TextColumnarBatchJniWriter.h"

#include <memory>
#include <string>

#include "jni_common.h"
#include "reader/common/UriInfo.h"
#include "reader/text/TextWriter.h"
#include "vector/vector.h"

using omniruntime::reader::text::TextWriter;

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_initializeWriter(
    JNIEnv* env, jobject, jobject options)
{
    JNI_FUNC_START
    jstring uriValue = static_cast<jstring>(
        env->CallObjectMethod(options, jsonMethodString, env->NewStringUTF("uri")));
    const char* uriChars = env->GetStringUTFChars(uriValue, JNI_FALSE);
    std::string uri(uriChars);
    env->ReleaseStringUTFChars(uriValue, uriChars);

    jstring schemeValue = static_cast<jstring>(
        env->CallObjectMethod(options, jsonMethodString, env->NewStringUTF("scheme")));
    const char* schemeChars = env->GetStringUTFChars(schemeValue, JNI_FALSE);
    std::string scheme(schemeChars);
    env->ReleaseStringUTFChars(schemeValue, schemeChars);

    jstring hostValue = static_cast<jstring>(
        env->CallObjectMethod(options, jsonMethodString, env->NewStringUTF("host")));
    const char* hostChars = env->GetStringUTFChars(hostValue, JNI_FALSE);
    std::string host(hostChars);
    env->ReleaseStringUTFChars(hostValue, hostChars);

    jstring pathValue = static_cast<jstring>(
        env->CallObjectMethod(options, jsonMethodString, env->NewStringUTF("path")));
    const char* pathChars = env->GetStringUTFChars(pathValue, JNI_FALSE);
    std::string path(pathChars);
    env->ReleaseStringUTFChars(pathValue, pathChars);

    jint port = env->CallIntMethod(options, jsonMethodInt, env->NewStringUTF("port"));
    UriInfo uriInfo(uri, scheme, path, host, std::to_string(port));
    auto writer = std::make_unique<TextWriter>();
    writer->Init(uriInfo);
    return reinterpret_cast<jlong>(writer.release());
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_write(
    JNIEnv* env, jobject, jlong writer, jlong vectorNativeId, jlong startPos, jlong endPos)
{
    JNI_FUNC_START
    auto* textWriter = reinterpret_cast<TextWriter*>(writer);
    auto* vector = reinterpret_cast<omniruntime::vec::BaseVector*>(vectorNativeId);
    if (textWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "Text writer is null.");
        return;
    }
    textWriter->Write(vector, startPos, endPos);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_close(
    JNIEnv* env, jobject, jlong writer)
{
    JNI_FUNC_START
    std::unique_ptr<TextWriter> textWriter(reinterpret_cast<TextWriter*>(writer));
    if (textWriter == nullptr) {
        return;
    }
    textWriter->Close();
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
