/**
 * Copyright (C) 2021-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

#include <jni.h>
#include <vector/vector_common.h>
#include <type/data_type_serializer.h>
#include "shuffle/splitter.h"

#ifndef SPARK_JNI_WRAPPER
#define SPARK_JNI_WRAPPER
#ifdef __cplusplus
extern "C" {
#endif
/*
 * Class:   com_huawei_boostkit_spark_jni_SparkJniWrapper 
 * Method:  nativeMake
 * Signature:   ()V
 */
JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_nativeMake(JNIEnv *env, jobject,
    jstring partitioning_name_jstr, jint num_partitions, jstring jInputType, jint jNumCols, jint buffer_size,
    jstring compression_type_jstr, jstring data_file_jstr, jint num_sub_dirs, jstring local_dirs_jstr,
    jlong compress_block_size, jint spill_batch_row, jlong task_spill_memory_threshold,
    jlong executor_spill_memory_threshold);

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_split(JNIEnv *env, jobject jObj,
    jlong splitter_id, jlong jVecBatchAddress);

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_rowSplit(JNIEnv *env, jobject jObj,
    jlong splitter_id, jlong jVecBatchAddress);

JNIEXPORT jobject JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_stop(JNIEnv *env, jobject,
    jlong splitter_id);

JNIEXPORT jobject JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_rowStop(JNIEnv *env, jobject,
    jlong splitter_id);

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_close(JNIEnv *env, jobject, jlong splitter_id);

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_OmniPlanEvaluatorJniWrapper_nativeValidateWithFailureReason(
    JNIEnv *env, jobject wrapper, jbyteArray planArray);

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_OmniPlanEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv *env, jobject wrapper, jbyteArray planArr, jobjectArray splitInfosArr, jobjectArray iterArr, jint stageId,
    jint partitionId, jlong taskId, jboolean saveInput, jstring spillDir);

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeHasNext(JNIEnv *env,
    jobject wrapper, jlong iterHandle);

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeTransform(JNIEnv *env,
    jobject wrapper, jlong iterHandle);

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeNext(JNIEnv *env,
    jobject wrapper, jlong iterHandle);

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeClose(JNIEnv *env,
    jobject wrapper, jlong iterHandle);

JNIEXPORT jlong JNICALL Java_org_apache_gluten_runtime_OmniRuntimeJniWrapper_createRuntime(JNIEnv *env, jclass,
    jstring jBackendType, jlong nmmHandle, jbyteArray sessionConf);

JNIEXPORT void JNICALL Java_org_apache_gluten_runtime_OmniRuntimeJniWrapper_releaseRuntime(JNIEnv *env, jclass,
    jlong ctxHandle);

JNIEXPORT jobject JNICALL Java_org_apache_gluten_metrics_OmniIteratorMetricsJniWrapper_nativeFetchMetrics(
    JNIEnv* env,
    jobject wrapper,
    jlong iterHandle);
#ifdef __cplusplus
}
#endif
#endif
