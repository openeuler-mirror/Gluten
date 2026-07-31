/**
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

#include "jni_common.h"
#include "deserializer.hh"
#include "common/common.h"
#include "common/debug.h"
#include "type/data_type.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/arrow_columnar_deserializer.h"
#include "shuffle/arrow_row_deserializer.h"

#include <cstring>

using namespace omniruntime::vec;

// ============================================================
// Parse context for Arrow shuffle deserialization
// ============================================================

// Columnar parse context — returned as jlong handle to Java
struct ColumnarParseContext {
    std::shared_ptr<arrow::Buffer> arrowFileBuffer;  // holds data lifetime
    ArrowFileHeader arrowHeader;
    const uint8_t* arrowCursor = nullptr;
    int64_t arrowRemaining = 0;
    int32_t arrowCurrentRowCount = 0;  // current batch row count
};

// Row parse context — returned as jlong handle to Java
struct RowParseContext {
    std::unique_ptr<RowShuffleBatchContext> arrowCtx;
};

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseInit(
    JNIEnv *env, jobject obj, jlong address, jint length)
{
    JNI_FUNC_START
    auto* ctx = new ColumnarParseContext();
    const auto* data = reinterpret_cast<const uint8_t*>(address);

    // Check for Arrow magic "OMSA"
    if (length < 4 || memcmp(data, kArrowShuffleMagic, 4) != 0) {
        delete ctx;
        OMNI_THROW("USER_ERROR", "columnarShuffleParseInit: Arrow magic 'OMSA' not found in data");
    }

    // Arrow path
    int64_t consumed = 0;
    auto result = ReadFileHeader(data, static_cast<int64_t>(length), &consumed);
    if (!result.ok()) {
        delete ctx;
        OMNI_THROW("USER_ERROR", "columnarShuffleParseInit: Arrow file header parse failed: {}",
                   result.status().ToString());
    }
    auto& header = *result;
    if (header.version != kArrowShuffleVersion) {
        delete ctx;
        OMNI_THROW("USER_ERROR",
                   "columnarShuffleParseInit: unsupported Arrow version {}, expected {}",
                   header.version, kArrowShuffleVersion);
    }
    if (header.layout != ShuffleLayout::COLUMNAR) {
        delete ctx;
        OMNI_THROW("USER_ERROR",
                   "columnarShuffleParseInit: layout mismatch, expected COLUMNAR but got ROW");
    }
    ctx->arrowHeader = std::move(header);
    // Create a buffer to hold data lifetime (zero-copy wrap)
    ctx->arrowFileBuffer = arrow::Buffer::Wrap(data, static_cast<int64_t>(length));
    ctx->arrowCursor = data + consumed;
    ctx->arrowRemaining = static_cast<int64_t>(length) - consumed;
    ctx->arrowCurrentRowCount = 0;

    return reinterpret_cast<jlong>(ctx);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseClose(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    auto* ctx = reinterpret_cast<ColumnarParseContext*>(address);
    delete ctx;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseVecCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    auto* ctx = reinterpret_cast<ColumnarParseContext*>(address);
    return static_cast<jint>(ctx->arrowHeader.schema.size());
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseRowCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    auto* ctx = reinterpret_cast<ColumnarParseContext*>(address);
    // Lazily peek at the next batch header to get rowCount
    if (ctx->arrowCurrentRowCount == 0 && ctx->arrowRemaining > 0) {
        int64_t consumed = 0;
        auto result = ReadColumnarBatch(ctx->arrowCursor, ctx->arrowRemaining,
                                        ctx->arrowHeader.schema, &consumed);
        if (!result.ok()) {
            OMNI_THROW("USER_ERROR", "columnarShuffleParseRowCount: failed to read batch: {}",
                       result.status().ToString());
        }
        ctx->arrowCurrentRowCount = result->rowCount;
        // Don't advance cursor — ParseBatch will re-read
    }
    return static_cast<jint>(ctx->arrowCurrentRowCount);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_columnarShuffleParseBatch(
    JNIEnv *env, jobject obj, jlong address, jintArray typeIdArray, jintArray precisionArray,
    jintArray scaleArray, jlongArray vecNativeIdArray)
{
    auto* ctx = reinterpret_cast<ColumnarParseContext*>(address);

    // Compute vecCount before JNI_FUNC_START so the VLA is accessible from
    // the JNI_FUNC_END_WITH_VECTORS catch block.
    int32_t vecCount = static_cast<int32_t>(ctx->arrowHeader.schema.size());
    int32_t rowCount = 0;
    ColumnarBatchBody arrowBatchBody;

    omniruntime::vec::BaseVector* vecs[vecCount]{};

    JNI_FUNC_START

    // Arrow path — read batch
    {
        int64_t consumed = 0;
        auto result = ReadColumnarBatch(ctx->arrowCursor, ctx->arrowRemaining,
                                        ctx->arrowHeader.schema, &consumed);
        if (!result.ok()) {
            OMNI_THROW("USER_ERROR", "columnarShuffleParseBatch: failed to read arrow batch: {}",
                       result.status().ToString());
        }
        arrowBatchBody = std::move(*result);
        ctx->arrowCursor += consumed;
        ctx->arrowRemaining -= consumed;
        ctx->arrowCurrentRowCount = 0;  // reset for next batch

        rowCount = arrowBatchBody.rowCount;
    }

    jint *typeIdArrayElements = env->GetIntArrayElements(typeIdArray, NULL);
    jint *precisionArrayElements = env->GetIntArrayElements(precisionArray, NULL);
    jint *scaleArrayElements = env->GetIntArrayElements(scaleArray, NULL);
    jlong *vecNativeIdArrayElements = env->GetLongArrayElements(vecNativeIdArray, NULL);

    // Create vectors from header schema, deserialize from buffer list
    {
        std::size_t bufIdx = 0;
        for (int32_t i = 0; i < vecCount; ++i) {
            const auto& desc = ctx->arrowHeader.schema[i];
            typeIdArrayElements[i] = static_cast<jint>(desc.typeId);
            precisionArrayElements[i] = static_cast<jint>(desc.precision);
            scaleArrayElements[i] = static_cast<jint>(desc.scale);

            auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(desc.typeId);
            if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
                auto dataType = DescriptorToOmniType(desc);
                vecs[i] = VectorHelper::CreateComplexVector(dataType.get(), rowCount);
            } else {
                vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
            }
            vecNativeIdArrayElements[i] = reinterpret_cast<jlong>(vecs[i]);

            DeserializeArrowBufferToOmniVector(desc, rowCount, arrowBatchBody.buffers, bufIdx, vecs[i]);
        }
    }

    env->ReleaseIntArrayElements(typeIdArray, typeIdArrayElements, 0);
    env->ReleaseIntArrayElements(precisionArray, precisionArrayElements, 0);
    env->ReleaseIntArrayElements(scaleArray, scaleArrayElements, 0);
    env->ReleaseLongArrayElements(vecNativeIdArray, vecNativeIdArrayElements, 0);
    JNI_FUNC_END_WITH_VECTORS(runtimeExceptionClass, vecs)
}

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseInit(
    JNIEnv *env, jobject obj, jlong address, jint length)
{
    JNI_FUNC_START
    auto* ctx = new RowParseContext();
    const auto* data = reinterpret_cast<const uint8_t*>(address);

    // Check for Arrow magic "OMSA"
    if (length < 4 || memcmp(data, kArrowShuffleMagic, 4) != 0) {
        delete ctx;
        OMNI_THROW("USER_ERROR", "rowShuffleParseInit: Arrow magic 'OMSA' not found in data");
    }

    // Arrow path
    auto result = RowShuffleParseInit(data, static_cast<int64_t>(length));
    if (!result.ok()) {
        delete ctx;
        OMNI_THROW("USER_ERROR", "rowShuffleParseInit: Arrow row parse init failed: {}",
                   result.status().ToString());
    }
    ctx->arrowCtx = std::move(*result);

    return reinterpret_cast<jlong>(ctx);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseClose(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    auto* ctx = reinterpret_cast<RowParseContext*>(address);
    // Arrow ctx is cleaned up by unique_ptr destructor
    delete ctx;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseVecCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    auto* ctx = reinterpret_cast<RowParseContext*>(address);
    return static_cast<jint>(ctx->arrowCtx->vecCnt);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jint JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseRowCount(
    JNIEnv *env, jobject obj, jlong address)
{
    JNI_FUNC_START
    auto* ctx = reinterpret_cast<RowParseContext*>(address);
    // Arrow's ParseNextBatch loads the batch head and sets rowCnt
    if (ctx->arrowCtx->rowCnt == 0 && ctx->arrowCtx->remaining > 0) {
        auto status = RowShuffleParseNextBatch(*ctx->arrowCtx);
        if (!status.ok()) {
            OMNI_THROW("USER_ERROR", "rowShuffleParseRowCount: failed to read next batch: {}",
                       status.ToString());
        }
    }
    return static_cast<jint>(ctx->arrowCtx->rowCnt);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_spark_serialize_ShuffleDataSerializerUtils_rowShuffleParseBatch(
    JNIEnv *env, jobject obj, jlong address, jintArray typeIdArray, jintArray precisionArray,
    jintArray scaleArray, jlongArray vecNativeIdArray)
{
    auto* ctx = reinterpret_cast<RowParseContext*>(address);
    int32_t vecCount = ctx->arrowCtx->vecCnt;
    int32_t rowCount = ctx->arrowCtx->rowCnt;

    omniruntime::vec::BaseVector* vecs[vecCount];
    std::vector<omniruntime::type::DataTypeId> omniDataTypeIds(vecCount);

    JNI_FUNC_START
    jint *typeIdArrayElements = env->GetIntArrayElements(typeIdArray, NULL);
    jint *precisionArrayElements = env->GetIntArrayElements(precisionArray, NULL);
    jint *scaleArrayElements = env->GetIntArrayElements(scaleArray, NULL);
    jlong *vecNativeIdArrayElements = env->GetLongArrayElements(vecNativeIdArray, NULL);

    // Arrow path — create vectors from schema, use RowShuffleParseBatch
    for (int32_t i = 0; i < vecCount; ++i) {
        const auto& desc = ctx->arrowCtx->header.schema[i];
        typeIdArrayElements[i] = static_cast<jint>(desc.typeId);
        precisionArrayElements[i] = static_cast<jint>(desc.precision);
        scaleArrayElements[i] = static_cast<jint>(desc.scale);
        omniDataTypeIds[i] = static_cast<omniruntime::type::DataTypeId>(desc.typeId);

        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(desc.typeId);
        if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
            auto dataType = DescriptorToOmniType(desc);
            vecs[i] = VectorHelper::CreateComplexVector(dataType.get(), rowCount);
        } else {
            vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = reinterpret_cast<jlong>(vecs[i]);
    }

    // Parse rows using RowShuffleParseBatch (reuses RowParser::ParseOneRow internally)
    RowShuffleParseBatch(*ctx->arrowCtx, vecs);

    env->ReleaseIntArrayElements(typeIdArray, typeIdArrayElements, 0);
    env->ReleaseIntArrayElements(precisionArray, precisionArrayElements, 0);
    env->ReleaseIntArrayElements(scaleArray, scaleArrayElements, 0);
    env->ReleaseLongArrayElements(vecNativeIdArray, vecNativeIdArrayElements, 0);
    JNI_FUNC_END_WITH_VECTORS(runtimeExceptionClass, vecs)
}
