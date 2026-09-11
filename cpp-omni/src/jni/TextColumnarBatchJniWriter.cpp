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
#include <stdexcept>
#include <string>
#include <vector>

#include "expression/HiveTypeParser.h"
#include "jni_common.h"
#include "reader/common/UriInfo.h"
#include "reader/text/TextFormatOptions.h"
#include "reader/text/TextWriter.h"
#include "type/data_type.h"
#include "vector/vector.h"

using omniruntime::reader::text::TextWriter;

namespace {

std::string GetJsonString(JNIEnv* env, jobject json, const char* key)
{
    jstring keyValue = env->NewStringUTF(key);
    jstring value = static_cast<jstring>(env->CallObjectMethod(json, jsonMethodString, keyValue));
    env->DeleteLocalRef(keyValue);
    if (value == nullptr) {
        return {};
    }
    const char* chars = env->GetStringUTFChars(value, nullptr);
    std::string result(chars);
    env->ReleaseStringUTFChars(value, chars);
    env->DeleteLocalRef(value);
    return result;
}

bool HasJsonKey(JNIEnv* env, jobject json, const char* key)
{
    jstring keyValue = env->NewStringUTF(key);
    const bool result = env->CallBooleanMethod(json, jsonMethodHas, keyValue) == JNI_TRUE;
    env->DeleteLocalRef(keyValue);
    return result;
}

void CopyOption(
    JNIEnv* env,
    jobject source,
    nlohmann::json& target,
    const char* sourceKey,
    const char* targetKey,
    const char* defaultValue = "")
{
    target[targetKey] = HasJsonKey(env, source, sourceKey)
        ? GetJsonString(env, source, sourceKey)
        : std::string(defaultValue);
}

omniruntime::type::RowTypePtr ParseRowType(
    const std::string& serializedTypes, const std::string& serializedNames)
{
    std::vector<std::string> names;
    std::vector<omniruntime::type::DataTypePtr> types;
    omniruntime::type::fbhive::HiveTypeParser parser;
    size_t begin = 0;
    while (begin <= serializedTypes.size()) {
        const auto end = serializedTypes.find('\x1f', begin);
        const auto typeName = serializedTypes.substr(
            begin, end == std::string::npos ? std::string::npos : end - begin);
        if (typeName.empty()) {
            break;
        }
        names.emplace_back("c" + std::to_string(names.size()));
        // Spark/Omni represents SQL DATE as days (DATE32). HiveTypeParser's historical
        // "date" mapping is DATE64 and does not match the vectors delivered by Spark writers.
        types.emplace_back(typeName == "date"
            ? omniruntime::type::Date32Type()
            : parser.parse(typeName));
        if (end == std::string::npos) {
            break;
        }
        begin = end + 1;
    }
    if (types.empty()) {
        throw std::runtime_error("Text writer schema is empty.");
    }
    if (!serializedNames.empty()) {
        names = nlohmann::json::parse(serializedNames).get<std::vector<std::string>>();
        if (names.size() != types.size()) {
            throw std::runtime_error("Text writer column names do not match its types.");
        }
    }
    return ROW(std::move(names), std::move(types));
}

std::shared_ptr<nlohmann::json> ParseTextOptions(JNIEnv* env, jobject options)
{
    auto parsed = std::make_shared<nlohmann::json>();
    CopyOption(env, options, *parsed, "text_source_kind", "text.source_kind");
    CopyOption(env, options, *parsed, "text_codec_kind", "text.codec_kind");
    CopyOption(env, options, *parsed, "text_charset", "text.charset", "UTF-8");
    CopyOption(env, options, *parsed, "text_line_separator", "text.line_separator");
    CopyOption(env, options, *parsed, "text_compression_codec", "text.compression_codec", "NONE");
    CopyOption(env, options, *parsed,
        "text_compression_block_size", "text.compression_block_size", "262144");
    CopyOption(env, options, *parsed, "text_session_timezone", "text.session_timezone");
    CopyOption(env, options, *parsed, "text_date_format", "text.date_format");
    CopyOption(env, options, *parsed,
        "text_timestamp_format_count", "text.timestamp_format_count", "0");
    const auto timestampFormatCount = std::stoul(
        parsed->at("text.timestamp_format_count").get<std::string>());
    for (size_t index = 0; index < timestampFormatCount; ++index) {
        const auto sourceKey = "text_timestamp_format_" + std::to_string(index);
        const auto targetKey = "text.timestamp_format_" + std::to_string(index);
        CopyOption(env, options, *parsed, sourceKey.c_str(), targetKey.c_str());
    }
    CopyOption(env, options, *parsed, "text_splitable", "text.splitable", "true");
    CopyOption(env, options, *parsed, "text_whole_text", "text.whole_text", "false");
    CopyOption(env, options, *parsed, "field_delimiter", "text.field_delimiter");
    CopyOption(env, options, *parsed, "nullValue", "text.null_literal", "\\N");
    CopyOption(env, options, *parsed, "text_escape_enabled", "text.escape_enabled", "false");
    CopyOption(env, options, *parsed, "escape", "text.escape_char");
    CopyOption(env, options, *parsed, "quote", "text.quote");
    CopyOption(env, options, *parsed, "text_parse_mode", "text.parse_mode", "PERMISSIVE");
    CopyOption(env, options, *parsed, "header", "text.skip_input_lines", "0");
    CopyOption(env, options, *parsed, "text_emit_header", "text.emit_header", "false");
    CopyOption(
        env,
        options,
        *parsed,
        "text_last_column_takes_rest",
        "text.last_column_takes_rest",
        "false");
    CopyOption(env, options, *parsed, "text_collection_delimiter", "text.collection_delimiter");
    CopyOption(env, options, *parsed, "text_map_key_delimiter", "text.map_key_delimiter");
    return parsed;
}

} // namespace

JNIEXPORT jlong JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_initializeWriter(
    JNIEnv* env, jobject, jobject options)
{
    JNI_FUNC_START
    const auto uri = GetJsonString(env, options, "uri");
    const auto scheme = GetJsonString(env, options, "scheme");
    const auto host = GetJsonString(env, options, "host");
    const auto path = GetJsonString(env, options, "path");

    jstring portKey = env->NewStringUTF("port");
    jint port = env->CallIntMethod(options, jsonMethodInt, portKey);
    env->DeleteLocalRef(portKey);
    UriInfo uriInfo(uri, scheme, path, host, std::to_string(port));
    const auto textOptions = omniruntime::reader::text::TextFormatOptions::FromJson(
        ParseTextOptions(env, options));
    auto writer = std::make_unique<TextWriter>(
        textOptions, ParseRowType(GetJsonString(env, options, "text_schema_types"),
            GetJsonString(env, options, "text_schema_names")));
    writer->Init(uriInfo);
    return reinterpret_cast<jlong>(writer.release());
    JNI_FUNC_END(runtimeExceptionClass)
}

JNIEXPORT void JNICALL
Java_com_huawei_boostkit_write_jni_TextColumnarBatchJniWriter_write(
    JNIEnv* env, jobject, jlong writer, jlongArray vectorNativeIds, jlong startPos, jlong endPos)
{
    JNI_FUNC_START
    auto* textWriter = reinterpret_cast<TextWriter*>(writer);
    if (textWriter == nullptr) {
        env->ThrowNew(runtimeExceptionClass, "Text writer is null.");
        return;
    }
    auto* nativeIds = env->GetLongArrayElements(vectorNativeIds, JNI_FALSE);
    const auto vectorCount = env->GetArrayLength(vectorNativeIds);
    std::vector<omniruntime::vec::BaseVector*> vectors;
    vectors.reserve(vectorCount);
    for (jsize index = 0; index < vectorCount; ++index) {
        vectors.emplace_back(
            reinterpret_cast<omniruntime::vec::BaseVector*>(nativeIds[index]));
    }
    env->ReleaseLongArrayElements(vectorNativeIds, nativeIds, JNI_ABORT);
    textWriter->Write(vectors, startPos, endPos);
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
