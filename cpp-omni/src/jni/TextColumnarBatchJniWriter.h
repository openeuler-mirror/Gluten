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

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_initializeWriter(
    JNIEnv* env, jobject object, jobject options);

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_write(
    JNIEnv* env, jobject object, jlong writer, jlong vectorNativeId, jlong startPos, jlong endPos);

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_close(
    JNIEnv* env, jobject object, jlong writer);

#ifdef __cplusplus
}
#endif
