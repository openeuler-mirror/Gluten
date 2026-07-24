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

#include "OrcColumnarBatchJniWriter.h"

#include <memory>
#include <string>
#include <functional>
#include <json/json.h>
#include <orc/Type.hh>
#include "jni_common.h"
#include "reader/orc/OrcFileOverride.hh"
#include "reader/orc/OmniWriter.hh"
#include "vector/vector_common.h"

using namespace omniruntime::vec;
using namespace omniruntime::type;
using namespace omniruntime::reader;
using namespace omniruntime::writer;

static constexpr int32_t DECIMAL_PRECISION_INDEX = 0;
static constexpr int32_t DECIMAL_SCALE_INDEX = 1;
static constexpr int32_t MINOR_VERSION_11 = 11;
static constexpr int32_t MINOR_VERSION_12 = 12;
static constexpr int32_t MAJOR_VERSION_0 = 0;

static std::string getJsonString(JNIEnv *env, jobject json, jmethodID stringMethod, const char *key)
{
    jstring keyStr = env->NewStringUTF(key);
    jstring valueStr = (jstring)env->CallObjectMethod(json, stringMethod, keyStr);
    env->DeleteLocalRef(keyStr);
    if (valueStr == NULL) {
        return "";
    }

    const char *chars = env->GetStringUTFChars(valueStr, NULL);
    std::string value(chars);
    env->ReleaseStringUTFChars(valueStr, chars);
    env->DeleteLocalRef(valueStr);
    return value;
}

static bool hasJsonKey(JNIEnv *env, jobject json, const char *key)
{
    jstring keyStr = env->NewStringUTF(key);
    jboolean hasKey = env->CallBooleanMethod(json, jsonMethodHas, keyStr);
    env->DeleteLocalRef(keyStr);
    return hasKey == JNI_TRUE;
}

static jint getJsonInt(JNIEnv *env, jobject json, const char *key)
{
    jstring keyStr = env->NewStringUTF(key);
    jint value = (jint)env->CallIntMethod(json, jsonMethodInt, keyStr);
    env->DeleteLocalRef(keyStr);
    return value;
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeOutputStream(
    JNIEnv *env, jobject jObj, jobject uriJson)
{
    JNI_FUNC_START
    jstring schemaJstr = (jstring)env->CallObjectMethod(uriJson, jsonMethodString, env->NewStringUTF("scheme"));
    const char *schemaPtr = env->GetStringUTFChars(schemaJstr, nullptr);
    std::string schemaStr(schemaPtr);
    env->ReleaseStringUTFChars(schemaJstr, schemaPtr);
    jstring fileJstr = (jstring)env->CallObjectMethod(uriJson, jsonMethodString, env->NewStringUTF("path"));
    const char *filePtr = env->GetStringUTFChars(fileJstr, nullptr);
    std::string fileStr(filePtr);
    env->ReleaseStringUTFChars(fileJstr, filePtr);
    jstring hostJstr = (jstring)env->CallObjectMethod(uriJson, jsonMethodString, env->NewStringUTF("host"));
    const char *hostPtr = env->GetStringUTFChars(hostJstr, nullptr);
    std::string hostStr(hostPtr);
    env->ReleaseStringUTFChars(hostJstr, hostPtr);
    jint port = (jint)env->CallIntMethod(uriJson, jsonMethodInt, env->NewStringUTF("port"));
    UriInfo uri{schemaStr, fileStr, hostStr, std::to_string(port)};
    std::unique_ptr<::orc::OutputStream> outputStream = writeFileOverride(uri);
    ::orc::OutputStream *outputStreamNew = outputStream.release();
    return (jlong)(outputStreamNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeSchemaType(
        JNIEnv *env,
        jobject jObj,
        jintArray orcTypeIds,
        jintArray childCounts,
        jobjectArray fieldNames,
        jobjectArray decimalParam,
        jint topLevelFieldCount)
{
    JNI_FUNC_START
    auto orcTypeIdPtr = env->GetIntArrayElements(orcTypeIds, JNI_FALSE);
    auto childCountPtr = env->GetIntArrayElements(childCounts, JNI_FALSE);
    if (orcTypeIdPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "Orc type ids should not be null.");
    }
    if (childCountPtr == NULL) {
        env->ThrowNew(runtimeExceptionClass, "Child counts should not be null.");
    }

    auto nodeCount = static_cast<int32_t>(env->GetArrayLength(orcTypeIds));
    if (nodeCount != static_cast<int32_t>(env->GetArrayLength(childCounts)) ||
        nodeCount != static_cast<int32_t>(env->GetArrayLength(fieldNames)) ||
        nodeCount != static_cast<int32_t>(env->GetArrayLength(decimalParam))) {
        env->ThrowNew(runtimeExceptionClass, "Schema node arrays must have the same length.");
    }
    auto writeType = createPrimitiveType(::orc::TypeKind::STRUCT);

    struct ParsedNode {
        std::string name;
        std::unique_ptr<::orc::Type> type;
    };

    std::function<ParsedNode(int32_t&)> parseNode = [&](int32_t& cursor) -> ParsedNode {
        if (cursor >= nodeCount) {
            env->ThrowNew(runtimeExceptionClass, "Schema node cursor out of bounds.");
            return ParsedNode{"", createPrimitiveType(::orc::TypeKind::STRING)};
        }

        const int32_t currentIndex = cursor;
        const auto typeKind = static_cast<::orc::TypeKind>(orcTypeIdPtr[currentIndex]);
        const int32_t childCount = childCountPtr[currentIndex];
        jstring fieldNameObj = (jstring)env->GetObjectArrayElement(fieldNames, currentIndex);
        const char *fieldNameChars = env->GetStringUTFChars(fieldNameObj, nullptr);
        std::string currentFieldName(fieldNameChars == nullptr ? "" : fieldNameChars);
        env->ReleaseStringUTFChars(fieldNameObj, fieldNameChars);
        env->DeleteLocalRef(fieldNameObj);
        cursor += 1;

        switch (typeKind) {
            case ::orc::TypeKind::LIST: {
                if (childCount != 1) {
                    env->ThrowNew(runtimeExceptionClass, "LIST type must have exactly one child.");
                }
                auto childNode = parseNode(cursor);
                return ParsedNode{currentFieldName, createListType(std::move(childNode.type))};
            }
            case ::orc::TypeKind::MAP: {
                if (childCount != 2) {
                    env->ThrowNew(runtimeExceptionClass, "MAP type must have exactly two children.");
                }
                auto keyNode = parseNode(cursor);
                auto valueNode = parseNode(cursor);
                return ParsedNode{
                    currentFieldName,
                    createMapType(std::move(keyNode.type), std::move(valueNode.type))
                };
            }
            case ::orc::TypeKind::STRUCT: {
                auto structType = createPrimitiveType(::orc::TypeKind::STRUCT);
                for (int32_t i = 0; i < childCount; ++i) {
                    auto childNode = parseNode(cursor);
                    const std::string childName = childNode.name.empty() ?
                        ("field_" + std::to_string(i)) : childNode.name;
                    structType->addStructField(childName, std::move(childNode.type));
                }
                return ParsedNode{currentFieldName, std::move(structType)};
            }
            case ::orc::TypeKind::DECIMAL: {
                auto decimalParamArray = (jintArray)env->GetObjectArrayElement(decimalParam, currentIndex);
                auto decimalParamArrayPtr = env->GetIntArrayElements(decimalParamArray, JNI_FALSE);
                const auto precision = decimalParamArrayPtr[DECIMAL_PRECISION_INDEX];
                const auto scale = decimalParamArrayPtr[DECIMAL_SCALE_INDEX];
                env->ReleaseIntArrayElements(decimalParamArray, decimalParamArrayPtr, JNI_ABORT);
                env->DeleteLocalRef(decimalParamArray);
                return ParsedNode{currentFieldName, ::orc::createDecimalType(precision, scale)};
            }
            default: {
                return ParsedNode{currentFieldName, createPrimitiveType(typeKind)};
            }
        }
    };

    int32_t cursor = 0;
    for (int32_t i = 0; i < topLevelFieldCount; ++i) {
        auto topNode = parseNode(cursor);
        const std::string topName = topNode.name.empty() ? ("col_" + std::to_string(i)) : topNode.name;
        writeType->addStructField(topName, std::move(topNode.type));
    }

    env->ReleaseIntArrayElements(childCounts, childCountPtr, JNI_ABORT);
    env->ReleaseIntArrayElements(orcTypeIds, orcTypeIdPtr, JNI_ABORT);
    ::orc::Type *writerTypeNew = writeType.release();
    return (jlong)(writerTypeNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT jlong JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_initializeWriter(
    JNIEnv *env, jobject jObj, jlong outputStream, jlong schemaType, jobject writerOptionsJson)
{
    JNI_FUNC_START
    // Set write options
    // other param should set here, like padding tolerance, columns use bloom
    // filter, bloom filter fpp ...
    ::orc::MemoryPool *pool = ::orc::getDefaultPool();
    ::orc::WriterOptions writerOptions;
    writerOptions.setMemoryPool(pool);

    // Parsing and setting file version
    jobject versionJosnObj =
        (jobject)env->CallObjectMethod(writerOptionsJson, jsonMethodJsonObj, env->NewStringUTF("file version"));
    jint majorJint = (jint)env->CallIntMethod(versionJosnObj, jsonMethodInt, env->NewStringUTF("major"));
    jint minorJint = (jint)env->CallIntMethod(versionJosnObj, jsonMethodInt, env->NewStringUTF("minor"));
    uint32_t major = (uint32_t)majorJint;
    uint32_t minor = (uint32_t)minorJint;
    if (minor == MINOR_VERSION_11 && major == 0) {
        writerOptions.setFileVersion(::orc::FileVersion::v_0_11());
    }
    else if (minor == MINOR_VERSION_12 && major == 0) {
        writerOptions.setFileVersion(::orc::FileVersion::v_0_12());
    }
    else {
        env->ThrowNew(runtimeExceptionClass, "Unsupported file version.");
    }

    jint compressionJint = (jint)env->CallIntMethod(writerOptionsJson, jsonMethodInt, env->NewStringUTF("compression"));
    writerOptions.setCompression(static_cast<::orc::CompressionKind>(compressionJint));

    jlong stripSizeJint =
        (jlong)env->CallLongMethod(writerOptionsJson, jsonMethodLong, env->NewStringUTF("strip size"));
    writerOptions.setStripeSize(stripSizeJint);

    jint rowIndexStrideJint =
        (jint)env->CallIntMethod(writerOptionsJson, jsonMethodInt, env->NewStringUTF("row index stride"));
    writerOptions.setRowIndexStride((uint64_t)rowIndexStrideJint);

    jint compressionStrategyJint =
        (jint)env->CallIntMethod(writerOptionsJson, jsonMethodInt, env->NewStringUTF("compression strategy"));
    writerOptions.setCompressionStrategy(static_cast<::orc::CompressionStrategy>(compressionStrategyJint));

    jstring timezoneKey = env->NewStringUTF("timezone");
    jstring timezoneJStr = (jstring)env->CallObjectMethod(writerOptionsJson, jsonMethodString, timezoneKey);
    env->DeleteLocalRef(timezoneKey);
    if (timezoneJStr != NULL) {
        const char *tzChars = env->GetStringUTFChars(timezoneJStr, NULL);
        std::string tzName(tzChars);
        writerOptions.setTimezoneName(tzName);
        env->ReleaseStringUTFChars(timezoneJStr, tzChars);
        env->DeleteLocalRef(timezoneJStr);
    } else {
        std::cout << "[warning] OrcColumnarBatchJniWriter : timezone not found in writerOptions" << std::endl;
    }

    std::unique_ptr<common::JulianGregorianRebase> timestampRebase;
    std::string rebaseTz = getJsonString(env, writerOptionsJson, jsonMethodString, "timestamp rebase tz");
    std::string rebaseSwitches = getJsonString(env, writerOptionsJson, jsonMethodString, "timestamp rebase switches");
    std::string rebaseDiffs = getJsonString(env, writerOptionsJson, jsonMethodString, "timestamp rebase diffs");
    if (!rebaseTz.empty() && !rebaseSwitches.empty() && !rebaseDiffs.empty()) {
        nlohmann::json rebaseJson;
        rebaseJson["tz"] = rebaseTz;
        rebaseJson["switches"] = rebaseSwitches;
        rebaseJson["diffs"] = rebaseDiffs;
        timestampRebase = common::BuildJulianGregorianRebase(rebaseJson);
    }

    ::orc::OutputStream *stream = (::orc::OutputStream *)outputStream;
    ::orc::Type *writeType = (::orc::Type *)schemaType;

    // Keep defaults compatible with older Java callers that do not provide the
    // parallel fields. Each native writer receives its own immutable snapshot.
    OmniWriterRuntimeOptions runtimeOptions;
    if (hasJsonKey(env, writerOptionsJson, "parallel serialize enabled")) {
        jint enabled = getJsonInt(env, writerOptionsJson, "parallel serialize enabled");
        runtimeOptions.parallelSerializeEnabled = enabled != 0;
    }
    if (hasJsonKey(env, writerOptionsJson, "parallel serialize max threads")) {
        jint maxThreads = getJsonInt(env, writerOptionsJson, "parallel serialize max threads");
        runtimeOptions.parallelSerializeMaxThreads =
            maxThreads > 0 ? static_cast<uint32_t>(maxThreads) : 1u;
    }

    std::unique_ptr<OmniWriter> writer = createOmniWriterWithTimestampRebase(
        (*writeType), stream, writerOptions, std::move(timestampRebase), runtimeOptions);
    OmniWriter *writerNew = writer.release();
    return (jlong)(writerNew);
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_write(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jint numRows)
{
    JNI_FUNC_START

    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    OmniWriter *writerPtr = (OmniWriter *)writer;
    std::vector<BaseVector *> colVecs;
    for (int i = 0; i < colNums; ++i) {
        if (!dataColumnsIdsPtr[i]) {
            continue;
        }
        colVecs.push_back((BaseVector *)vecNativeIdPtr[i]);
    }
    std::unique_ptr<RowVector> rowVec = std::make_unique<RowVector>(numRows, colVecs);
    writerPtr->add(rowVec.get(), 0, numRows);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_splitWrite(
    JNIEnv *env, jobject jObj, jlong writer, jlongArray vecNativeId, jintArray omniTypes, jbooleanArray dataColumnsIds,
    jlong startPos, jlong endPos)
{
    JNI_FUNC_START
    auto vecNativeIdPtr = env->GetLongArrayElements(vecNativeId, JNI_FALSE);
    auto colNums = env->GetArrayLength(vecNativeId);
    auto omniTypesPtr = env->GetIntArrayElements(omniTypes, JNI_FALSE);
    auto dataColumnsIdsPtr = env->GetBooleanArrayElements(dataColumnsIds, JNI_FALSE);
    auto writeRows = endPos - startPos;
    OmniWriter *writerPtr = (OmniWriter *)writer;
    std::vector<BaseVector *> colVecs;
    for (int i = 0; i < colNums; ++i) {
        if (!dataColumnsIdsPtr[i]) {
            continue;
        }
        colVecs.push_back((BaseVector *)vecNativeIdPtr[i]);
    }
    std::unique_ptr<RowVector> rowVec = std::make_unique<RowVector>(writeRows, colVecs);
    writerPtr->add(rowVec.get(), startPos, endPos);
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}

JNIEXPORT void JNICALL Java_com_huawei_boostkit_write_jni_OrcColumnarBatchJniWriter_close(JNIEnv *env, jobject jObj,
                                                                                          jlong outputStream,
                                                                                          jlong schemaType,
                                                                                          jlong writer)
{
    JNI_FUNC_START

    OmniWriter *writerPtr = (OmniWriter *)writer;
    if (writerPtr == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for writer");
    }

    try {
        writerPtr->close();
    }
    catch (const char *e) {
        std::string errorMsg = "close columnar writer fail:";
        errorMsg += e;
        env->ThrowNew(runtimeExceptionClass, errorMsg.c_str());
    }

    ::orc::Type *schemaTypePtr = (::orc::Type *)schemaType;
    if (schemaTypePtr == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "delete nullptr error for write schema type");
    }
    delete schemaTypePtr;

    delete writerPtr;
    JNI_FUNC_END_VOID(runtimeExceptionClass)
}
