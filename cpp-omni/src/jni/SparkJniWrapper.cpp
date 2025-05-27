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

#include <fcntl.h>
#include <unistd.h>

#include "io/SparkFile.hh"
#include "io/ColumnWriter.hh"
#include "jni_common.h"
#include "SparkJniWrapper.hh"
#include "compute/OmniPlanConverter.h"
#include "substrait/SubstraitToOmniPlanValidator.h"
#include "compute/WholeStageResultIterator.h"
#include "compute/Runtime.h"
#include "config/OmniConfig.h"

using namespace spark;
using namespace google::protobuf::io;
using namespace omniruntime::vec;

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_nativeMake(JNIEnv *env, jobject,
    jstring partitioning_name_jstr, jint num_partitions, jstring jInputType, jint jNumCols, jint buffer_size,
    jstring compression_type_jstr, jstring data_file_jstr, jint num_sub_dirs, jstring local_dirs_jstr,
    jlong compress_block_size, jint spill_batch_row, jlong task_spill_memory_threshold,
    jlong executor_spill_memory_threshold)
{
    JNI_FUNC_START
        if (partitioning_name_jstr == nullptr) {
            env->ThrowNew(runtimeExceptionClass, std::string("Short partitioning name can't be null").c_str());
            return 0;
        }
        if (jInputType == nullptr) {
            env->ThrowNew(runtimeExceptionClass, std::string("input types can't be null").c_str());
            return 0;
        }

        const char *inputTypeCharPtr = env->GetStringUTFChars(jInputType, JNI_FALSE);
        DataTypes inputVecTypes = Deserialize(inputTypeCharPtr);
        const int32_t *inputVecTypeIds = inputVecTypes.GetIds();
        //
        std::vector<DataTypePtr> inputDataTypes = inputVecTypes.Get();
        int32_t size = inputDataTypes.size();
        uint32_t *inputDataPrecisions = new uint32_t[size];
        uint32_t *inputDataScales = new uint32_t[size];
        for (int i = 0; i < size; ++i) {
            if (inputDataTypes[i]->GetId() == OMNI_DECIMAL64 || inputDataTypes[i]->GetId() == OMNI_DECIMAL128) {
                inputDataScales[i] = std::dynamic_pointer_cast<DecimalDataType>(inputDataTypes[i])->GetScale();
                inputDataPrecisions[i] = std::dynamic_pointer_cast<DecimalDataType>(inputDataTypes[i])->GetPrecision();
            }
        }
        inputDataTypes.clear();

        InputDataTypes inputDataTypesTmp;
        inputDataTypesTmp.inputVecTypeIds = (int32_t*)inputVecTypeIds;
        inputDataTypesTmp.inputDataPrecisions = inputDataPrecisions;
        inputDataTypesTmp.inputDataScales = inputDataScales;

        if (data_file_jstr == nullptr) {
            env->ThrowNew(runtimeExceptionClass, std::string("Shuffle DataFile can't be null").c_str());
            return 0;
        }
        if (local_dirs_jstr == nullptr) {
            env->ThrowNew(runtimeExceptionClass, std::string("Shuffle DataFile can't be null").c_str());
            return 0;
        }

        auto partitioning_name_c = env->GetStringUTFChars(partitioning_name_jstr, JNI_FALSE);
        auto partitioning_name = std::string(partitioning_name_c);
        env->ReleaseStringUTFChars(partitioning_name_jstr, partitioning_name_c);

        auto splitOptions = SplitOptions::Defaults();
        if (buffer_size > 0) {
            splitOptions.buffer_size = buffer_size;
        }
        if (num_sub_dirs > 0) {
            splitOptions.num_sub_dirs = num_sub_dirs;
        }
        if (compression_type_jstr != NULL) {
            auto compression_type_result = GetCompressionType(env, compression_type_jstr);
            splitOptions.compression_type = compression_type_result;
        }

        auto data_file_c = env->GetStringUTFChars(data_file_jstr, JNI_FALSE);
        splitOptions.data_file = std::string(data_file_c);
        env->ReleaseStringUTFChars(data_file_jstr, data_file_c);

        auto local_dirs = env->GetStringUTFChars(local_dirs_jstr, JNI_FALSE);
        setenv("NATIVESQL_SPARK_LOCAL_DIRS", local_dirs, 1);
        env->ReleaseStringUTFChars(local_dirs_jstr, local_dirs);

        if (spill_batch_row > 0) {
            splitOptions.spill_batch_row_num = spill_batch_row;
        }
        if (task_spill_memory_threshold > 0) {
            splitOptions.task_spill_mem_threshold = task_spill_memory_threshold;
        }
        if (executor_spill_memory_threshold > 0) {
            splitOptions.executor_spill_mem_threshold = executor_spill_memory_threshold;
        }
        if (compress_block_size > 0) {
            splitOptions.compress_block_size = compress_block_size;
        }

        jobject thread = env->CallStaticObjectMethod(threadClass, currentThread);
        if (thread == NULL) {
            std::cout << "Thread.currentThread() return NULL" << std::endl;
        } else {
            jlong sid = env->CallLongMethod(thread, threadGetId);
            splitOptions.thread_id = (int64_t)sid;
        }

        auto splitter = Splitter::Make(partitioning_name, inputDataTypesTmp, jNumCols, num_partitions,
            std::move(splitOptions));
        return reinterpret_cast<intptr_t>(static_cast<void*>(splitter));
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_split(JNIEnv *env, jobject jObj,
    jlong splitter_addr, jlong jVecBatchAddress)
{
    auto splitter = reinterpret_cast<Splitter*>(splitter_addr);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
        env->ThrowNew(runtimeExceptionClass, error_message.c_str());
        return -1;
    }

    auto vecBatch = (VectorBatch*)jVecBatchAddress;
    splitter->SetInputVecBatch(vecBatch);
    JNI_FUNC_START
        splitter->Split(*vecBatch);
        return 0L;
    JNI_FUNC_END_WITH_VECBATCH(runtimeExceptionClass, splitter->GetInputVecBatch())
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_rowSplit(JNIEnv *env, jobject jObj,
    jlong splitter_addr, jlong jVecBatchAddress)
{
    auto splitter = reinterpret_cast<Splitter*>(splitter_addr);
    if (!splitter) {
        std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
        env->ThrowNew(runtimeExceptionClass, error_message.c_str());
        return -1;
    }

    auto vecBatch = (VectorBatch*)jVecBatchAddress;
    splitter->SetInputVecBatch(vecBatch);
    JNI_FUNC_START
        splitter->SplitByRow(vecBatch);
        return 0L;
    JNI_FUNC_END_WITH_VECBATCH(runtimeExceptionClass, splitter->GetInputVecBatch())
}

JNIEXPORT jobject JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_stop(JNIEnv *env, jobject,
    jlong splitter_addr)
{
    JNI_FUNC_START
        auto splitter = reinterpret_cast<Splitter*>(splitter_addr);
        if (!splitter) {
            std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
            env->ThrowNew(runtimeExceptionClass, error_message.c_str());
            return nullptr;
        }
        splitter->Stop();

        const auto &partition_length = splitter->PartitionLengths();
        auto partition_length_arr = env->NewLongArray(partition_length.size());
        auto src = reinterpret_cast<const jlong*>(partition_length.data());
        env->SetLongArrayRegion(partition_length_arr, 0, partition_length.size(), src);
        jobject split_result = env->NewObject(splitResultClass, splitResultConstructor, splitter->TotalComputePidTime(),
            splitter->TotalWriteTime(), splitter->TotalSpillTime(), splitter->TotalBytesWritten(),
            splitter->TotalBytesSpilled(), partition_length_arr);

        return split_result;
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jobject JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_rowStop(JNIEnv *env, jobject,
    jlong splitter_addr)
{
    JNI_FUNC_START
        auto splitter = reinterpret_cast<Splitter*>(splitter_addr);
        if (!splitter) {
            std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
            env->ThrowNew(runtimeExceptionClass, error_message.c_str());
            return nullptr;
        }
        splitter->StopByRow();

        const auto &partition_length = splitter->PartitionLengths();
        auto partition_length_arr = env->NewLongArray(partition_length.size());
        auto src = reinterpret_cast<const jlong*>(partition_length.data());
        env->SetLongArrayRegion(partition_length_arr, 0, partition_length.size(), src);
        jobject split_result = env->NewObject(splitResultClass, splitResultConstructor, splitter->TotalComputePidTime(),
            splitter->TotalWriteTime(), splitter->TotalSpillTime(), splitter->TotalBytesWritten(),
            splitter->TotalBytesSpilled(), partition_length_arr);

        return split_result;
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_spark_jni_SparkJniWrapper_close(JNIEnv *env, jobject,
    jlong splitter_addr)
{
    JNI_FUNC_START
        auto splitter = reinterpret_cast<Splitter*>(splitter_addr);
        if (!splitter) {
            std::string error_message = "Invalid splitter id " + std::to_string(splitter_addr);
            env->ThrowNew(runtimeExceptionClass, error_message.c_str());
        }
        delete splitter;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

inline uint8_t *getByteArrayElementsSafe(JNIEnv *env, jbyteArray array)
{
    auto nativeArray = env->GetByteArrayElements(array, nullptr);
    return reinterpret_cast<uint8_t*>(nativeArray);
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_PlanEvaluatorJniWrapper_nativeValidateWithFailureReason(
    JNIEnv *env, jobject wrapper, jbyteArray planArray)
{
    JNI_FUNC_START
        auto planData = getByteArrayElementsSafe(env, planArray);
        auto planSize = env->GetArrayLength(planArray);

        ::substrait::Plan subPlan;
        CodedInputStream codedStream{planData, planSize};
        codedStream.SetRecursionLimit(100000);
        ::substrait::Plan substraitPlan;
        substraitPlan.ParseFromCodedStream(&codedStream);

        auto pool = GetMemoryPool();
        omniruntime::SubstraitToOmniPlanValidator planValidator(pool);

        try {
            auto isSupported = planValidator.Validate(subPlan);
            auto logs = planValidator.GetValidateLog();
            std::string concatLog;
            for (int i = 0; i < logs.size(); i++) {
                concatLog += logs[i] + "@";
            }
            return env->NewObject(infoCls, method, isSupported, env->NewStringUTF(concatLog.c_str()));
        } catch (std::invalid_argument &e) {
            auto isSupported = false;
            return env->NewObject(infoCls, method, isSupported, env->NewStringUTF(""));
        }
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_OmniPlanEvaluatorJniWrapper_nativeCreateKernelWithIterator(
    JNIEnv *env, jobject wrapper, jbyteArray planArr, jobjectArray splitInfosArr, jobjectArray iterArr, jint stageId,
    jint partitionId, jlong taskId, jboolean saveInput, jstring spillDir)
{
    JNI_FUNC_START
        auto ctx = GetRuntime(env, wrapper);
        auto &conf = ctx->GetConfMap();

        auto buf = getByteArrayElementsSafe(env, planArr);
        auto planSize = env->GetArrayLength(planArr);
        ctx->ParsePlan(buf, planSize, std::nullopt);

        // Handle the Java iters
        jsize itersLen = env->GetArrayLength(iterArr);
        std::vector<std::shared_ptr<omniruntime::ResultIterator>> inputIters;
        for (int idx = 0; idx < itersLen; idx++) {
            jobject iter = env->GetObjectArrayElement(iterArr, idx);
            auto arrayIter = std::make_unique<JniColumnarBatchIterator>(env, iter);
            auto resultIter = std::make_shared<omniruntime::ResultIterator>(std::move(arrayIter));
            inputIters.push_back(std::move(resultIter));
        }
        auto spillDirStr = JStringToCString(env, spillDir);
        auto resultIterator = ctx->CreateResultIterator(spillDirStr, inputIters, conf);
        return reinterpret_cast<long>(resultIterator.release());
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jboolean JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeHasNext(JNIEnv *env,
    jobject wrapper, jlong iterHandle)
{
    JNI_FUNC_START
        const auto iter = reinterpret_cast<omniruntime::ResultIterator*>(iterHandle);
        if (iter == nullptr) {
            const std::string errorMessage = "When HasNext() is called on a closed iterator,"
                " an exception is thrown. To prevent this, consider using the protectInvocationFlow() "
                "method when creating the iterator in scala side. "
                "This will allow the HasNext() method to be called multiple times without issue.";
            throw omniruntime::exception::OmniException(errorMessage);
        }
        return iter->HasNext();
    JNI_FUNC_END(runtimeExceptionClass)
}

static jobject Transform(JNIEnv *env, VectorBatch &result)
{
    int32_t vecCount = result.GetVectorCount();
    int64_t vecAddresses[vecCount];
    int32_t encodings[vecCount];
    int32_t dataTypeIds[vecCount];
    int64_t valueBufAddrs[vecCount];
    int64_t nullBufAddrs[vecCount];
    int64_t offsetsBufAddrs[vecCount];
    for (int32_t i = 0; i < vecCount; ++i) {
        BaseVector *vector = result.Get(i);
        vecAddresses[i] = reinterpret_cast<uintptr_t>(vector);
        dataTypeIds[i] = vector->GetTypeId();
        encodings[i] = vector->GetEncoding();
        // By default, all 3 buf arrays will have a value,
        // if not, it will be 0, which means a null pointer.
        valueBufAddrs[i] = reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetValues(vector));
        nullBufAddrs[i] = reinterpret_cast<uintptr_t>(omniruntime::vec::unsafe::UnsafeBaseVector::GetNulls(vector));
        offsetsBufAddrs[i] = reinterpret_cast<uintptr_t>(VectorHelper::UnsafeGetOffsetsAddr(vector));
    }

    // set vector addresses parameter to vector batch construct.
    jlongArray jVecAddresses = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecAddresses, 0, vecCount, vecAddresses);

    // set vector encoding
    jintArray jVecEncodingIds = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jVecEncodingIds, 0, vecCount, encodings);

    // set vector type ids parameter to vector batch construct.
    jintArray jDataTypeIds = env->NewIntArray(vecCount);
    env->SetIntArrayRegion(jDataTypeIds, 0, vecCount, dataTypeIds);

    // set vector value buf address
    jlongArray jVecValueBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecValueBufAddrs, 0, vecCount, valueBufAddrs);

    // set vec null buf address
    jlongArray jVecNullBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecNullBufAddrs, 0, vecCount, nullBufAddrs);

    // set vec offsets buf address
    jlongArray jVecOffsetsBufAddrs = env->NewLongArray(vecCount);
    env->SetLongArrayRegion(jVecOffsetsBufAddrs, 0, vecCount, offsetsBufAddrs);

    // create vector batch java object.
    jobject obj = env->NewObject(vecBatchCls, vecBatchInitMethodId, (jlong)((int64_t)(&result)), jVecAddresses,
        jVecValueBufAddrs, jVecNullBufAddrs, jVecOffsetsBufAddrs, jVecEncodingIds, jDataTypeIds, result.GetRowCount());
    return obj;
}

JNIEXPORT jobject JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeTransform(JNIEnv *env,
    jobject wrapper, jlong batchHandle)
{
    JNI_FUNC_START
        const auto batch = reinterpret_cast<VectorBatch*>(batchHandle);
        if (batch == nullptr) {
            const std::string errorMessage = "vec batch is nullptr";
            env->ThrowNew(runtimeExceptionClass, errorMessage.c_str());
        }
        jobject result = nullptr;
        result = Transform(env, *batch);
        return result;
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeNext(JNIEnv *env,
    jobject wrapper, jlong iterHandle)
{
    JNI_FUNC_START
        const auto iter = reinterpret_cast<omniruntime::ResultIterator*>(iterHandle);
        if (!iter->HasNext()) {
            return -1;
        }

        VectorBatch *batch = iter->Next();
        return reinterpret_cast<intptr_t>(batch);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_org_apache_gluten_runtime_OmniRuntimeJniWrapper_createRuntime(JNIEnv *env, jclass,
    jstring jBackendType, jlong nmmHandle, jbyteArray sessionConf)
{
    JNI_FUNC_START
        auto safeArray = getByteArrayElementsSafe(env, sessionConf);
        auto length = env->GetArrayLength(sessionConf);
        auto sparkConf = omniruntime::ParseConfMap(safeArray, length);

        auto runtime = std::make_unique<omniruntime::Runtime>("omni", sparkConf);
        return reinterpret_cast<jlong>(runtime.release());
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_org_apache_gluten_runtime_OmniRuntimeJniWrapper_releaseRuntime(JNIEnv *env, jclass,
    jlong ctxHandle) {}

JNIEXPORT void JNICALL Java_org_apache_gluten_vectorized_OmniColumnarBatchOutIterator_nativeClose(
    JNIEnv *env,
    jobject wrapper,
    jlong iterHandle)
{
    JNI_FUNC_START
        const auto iter = reinterpret_cast<omniruntime::ResultIterator *>(iterHandle);
        if (iter == nullptr) {
            const std::string errorMessage = "ResultIterator is nullptr";
            env->ThrowNew(runtimeExceptionClass, errorMessage.c_str());
        }
        delete iter;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
