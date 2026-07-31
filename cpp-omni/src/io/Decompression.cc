/**
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

#include "Decompression.hh"
#include <memory>
#include <stdexcept>
#include <cstring>
#include <vector/vector_common.h>
#include "common/debug.h"
#include "shuffle/arrow_frame.h"
#include "shuffle/arrow_columnar_deserializer.h"
#include "shuffle/arrow_row_deserializer.h"

namespace spark {

bool DecompressionStream::ensureBufferHasData(JNIEnv* env)
{
    while (uncompressedCursor_ >= uncompressedLimit_) {
        if (finishedReading_) {
            return false;
        }
        if (!loadNextUncompressedChunk(env)) {
            return false;
        }
    }
    return true;
}

bool DecompressionStream::consumeBytes(JNIEnv* env, void* dest, int32_t n)
{
    if (n < 0) {
        return false;
    }
    auto* out = static_cast<char*>(dest);
    int32_t got = 0;
    while (got < n) {
        if (!ensureBufferHasData(env)) {
            return false;
        }
        const size_t avail = uncompressedLimit_ - uncompressedCursor_;
        const int32_t take = static_cast<int32_t>(std::min(avail, static_cast<size_t>(n - got)));
        memcpy(out + got, uncompressed.data() + uncompressedCursor_, static_cast<size_t>(take));
        uncompressedCursor_ += static_cast<size_t>(take);
        got += take;
    }
    return true;
}

int32_t DecompressionStream::readSize(JNIEnv* env)
{
    // 4 bytes read from the input stream, combined into a 32-bit data size
    char hdr[4];
    if (!consumeBytes(env, hdr, 4)) {
        return -1;
    }
    const int32_t dataSize = static_cast<int32_t>(
        (static_cast<uint32_t>(static_cast<uint8_t>(hdr[0])) << BYTE_3_OFFSET) |
        (static_cast<uint32_t>(static_cast<uint8_t>(hdr[1])) << BYTE_2_OFFSET) |
        (static_cast<uint32_t>(static_cast<uint8_t>(hdr[2])) << BYTE_1_OFFSET) |
        static_cast<uint32_t>(static_cast<uint8_t>(hdr[3])));
    return dataSize;
}

std::pair<char*, int32_t> DecompressionStream::decompress(JNIEnv* env, int32_t dataSize)
{
    if (dataSize <= 0) {
        return std::make_pair(nullptr, -1);
    }
    if (uncompress.size() < static_cast<size_t>(dataSize)) {
        uncompress.resize(static_cast<size_t>(dataSize));
    }
    if (!consumeBytes(env, uncompress.data(), dataSize)) {
        return std::make_pair(nullptr, -1);
    }
    return std::make_pair(uncompress.data(), dataSize);
}

bool DecompressionStream::loadNextFramedFromWire(JNIEnv* env)
{
    // 3 bytes read from the header for compressed size and original flag
    char hbuf[3];
    jlong ret = env->CallLongMethod(dIn, readByteMethod, reinterpret_cast<jlong>(hbuf), 3);
    if (ret < 3) {
        finishedReading_ = true;
        return false;
    }
    const int h0 = hbuf[0] & 0xff;
    const int h1 = hbuf[1] & 0xff;
    const int h2 = hbuf[2] & 0xff;
    const bool isOriginal = (h0 & 0x01) == 1;
    const int32_t chunkLength = (h2 << 15) | (h1 << 7) | (h0 >> 1);
    if (chunkLength <= 0) {
        throw std::runtime_error("invalid compression chunk length");
    }

    std::vector<char> compressed(static_cast<size_t>(chunkLength));
    jlong readBytes = 0;
    while (readBytes < chunkLength) {
        ret = env->CallLongMethod(dIn, readByteMethod,
            reinterpret_cast<jlong>(compressed.data() + readBytes),
            static_cast<jlong>(chunkLength - readBytes));
        if (ret == -1 || ret == 0) {
            throw std::runtime_error("failed to read chunk!");
        }
        readBytes += ret;
    }

    uncompressedCursor_ = 0;
    if (isOriginal) {
        uncompressed = std::move(compressed);
        uncompressedLimit_ = uncompressed.size();
        return true;
    }
    if (output == nullptr) {
        output = new char[static_cast<size_t>(shuffleCompressBlockSize)];
    }
    std::pair<char*, int32_t> decoded = doDecompression(compressed.data(), chunkLength);
    uncompressed.assign(decoded.first, decoded.first + decoded.second);
    uncompressedLimit_ = uncompressed.size();
    return true;
}

bool UncompressionStream::loadNextUncompressedChunk(JNIEnv* env)
{
    const size_t cap = static_cast<size_t>(std::max<int64_t>(shuffleCompressBlockSize, 4096));
    if (uncompressed.size() < cap) {
        uncompressed.resize(cap);
    }
    jlong readTotal = 0;
    while (readTotal < static_cast<jlong>(cap)) {
        const jlong ret = env->CallLongMethod(dIn, readByteMethod,
            reinterpret_cast<jlong>(uncompressed.data() + readTotal),
            static_cast<jlong>(cap - static_cast<size_t>(readTotal)));
        if (ret <= 0) {
            break;
        }
        readTotal += ret;
    }
    if (readTotal <= 0) {
        finishedReading_ = true;
        return false;
    }
    uncompressedCursor_ = 0;
    uncompressedLimit_ = static_cast<size_t>(readTotal);
    return true;
}


std::pair<char*, int32_t> LZ4DecompressionStream::doDecompression(char* input, int32_t inputLength) {
    int actualLength = LZ4_decompress_safe(input, output, inputLength, shuffleCompressBlockSize);
    if (actualLength < 0) {
        throw std::runtime_error("LZ4 decompression failed");
    }
    return std::make_pair(output, actualLength);
}

std::pair<char*, int32_t> SnappyDecompressionStream::doDecompression(char* input, int32_t inputLength) {
    size_t unCompressedSize;
    if (!snappy::GetUncompressedLength(input, inputLength, &unCompressedSize)) {
        throw std::runtime_error("Failed to get uncompressed length.");
    }

    if (!snappy::RawUncompress(input, inputLength, output)) {
        throw std::runtime_error("Failed to decompress data.");
    }
    return std::make_pair(output, unCompressedSize);
}

std::pair<char*, int32_t> ZlibDecompressionStream::doDecompression(char* input, int32_t inputLength) {

    z_stream stream;
    memset(&stream, 0, sizeof(stream));
    stream.zalloc = Z_NULL;
    stream.zfree = Z_NULL;
    stream.opaque = Z_NULL;

    int err = inflateInit2(&stream, -15);
    if (err != Z_OK) {
        throw std::runtime_error("Failed to initialize zlib decompression stream: " + std::string(zError(err)));
    }

    stream.next_in = (Bytef*)input;
    stream.avail_in = inputLength;
    stream.next_out = (Bytef*)output;
    stream.avail_out = shuffleCompressBlockSize;

    err = inflate(&stream, Z_NO_FLUSH);
    if (err != Z_STREAM_END && err != Z_OK) {
        inflateEnd(&stream);
        throw std::runtime_error("Failed to decompress data: " + std::string(stream.msg));
    }

    // Clean up the decompression stream
    err = inflateEnd(&stream);
    if (err != Z_OK) {
        throw std::runtime_error("Failed to clean up zlib decompression stream: " + std::string(zError(err)));
    }
    return std::make_pair(output, stream.total_out);
}

std::pair<char*, int32_t> ZstdDecompressionStream::doDecompression(char* input, int32_t inputLength) {
    auto actualLength = ZSTD_getDecompressedSize(input, inputLength);
    if (actualLength == 0) {
        throw std::runtime_error("ZSTD decompression size failed");
    }

    auto retCode = ZSTD_decompress(output, actualLength, input, inputLength);
    if (ZSTD_isError(retCode)) {
        throw std::runtime_error("ZSTD decompression failed:" + std::string(ZSTD_getErrorName(retCode)));
    }
    return std::make_pair(output, actualLength);
}

ShuffleReaderDeserializer::ShuffleReaderDeserializer(JNIEnv* env, jobject jniIn,
    CompressionKind codec, int64_t shuffleCompressBlockSize, jboolean isRowShuffle)
: env(env), shuffleCompressBlockSize(shuffleCompressBlockSize), isRowShuffle(JNI_TRUE == isRowShuffle)
{
    if (env->GetJavaVM(&vm_) != JNI_OK) {
        throw std::runtime_error("GetJavaVM failed");
    }
    this->jniIn = env->NewGlobalRef(jniIn);
    switch (static_cast<int64_t>(codec)) {
        case CompressionKind_LZ4: {
            this->decompressionStream = std::make_unique<LZ4DecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_SNAPPY: {
            this->decompressionStream = std::make_unique<SnappyDecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_ZLIB: {
            this->decompressionStream = std::make_unique<ZlibDecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_ZSTD: {
            this->decompressionStream = std::make_unique<ZstdDecompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        case CompressionKind_NONE: {
            this->decompressionStream = std::make_unique<UncompressionStream>(this->jniIn, this->shuffleCompressBlockSize);
            break;
        }
        default:
            throw std::logic_error("decompression codec not supported");
    }
}

int32_t DecompressionStream::createResult(JNIEnv *env, int rowCount, int vecCount,
    jint* typeIdArrayElements, jint* precisionArrayElements,
    jint* scaleArrayElements, jlong* vecNativeIdArrayElements)
{
    this->result = env->NewObject(metaInfoClass, ctor);
    if (result == nullptr) return -1;

    jintArray typeIdsArr = env->NewIntArray(vecCount);
    jintArray precArr    = env->NewIntArray(vecCount);
    jintArray scaleArr   = env->NewIntArray(vecCount);
    jlongArray vecIdArr  = env->NewLongArray(vecCount);

    env->SetIntArrayRegion(typeIdsArr, 0, vecCount, typeIdArrayElements);
    env->SetIntArrayRegion(precArr,    0, vecCount, precisionArrayElements);
    env->SetIntArrayRegion(scaleArr,   0, vecCount, scaleArrayElements);
    env->SetLongArrayRegion(vecIdArr,  0, vecCount, vecNativeIdArrayElements);

    // === 5. 设置字段值 ===
    env->SetObjectField(result, fidTypeIds, typeIdsArr);
    env->SetObjectField(result, fidPrec,    precArr);
    env->SetObjectField(result, fidScales,  scaleArr);
    env->SetObjectField(result, fidVecIds,  vecIdArr);
    env->SetIntField(result, fidRowCount, rowCount);
    env->SetIntField(result, fidVecCount, vecCount);

    return rowCount;
}

// Arrow columnar batch parse — reads file header, reads first batch, deserializes to Omni vectors
int32_t DecompressionStream::columnarShuffleParseArrowBatch(
    JNIEnv *env, const char* data, int32_t dataSize)
{
    const auto* rawData = reinterpret_cast<const uint8_t*>(data);

    // Read file header
    int64_t consumed = 0;
    auto headerResult = ReadFileHeader(rawData, static_cast<int64_t>(dataSize), &consumed);
    if (!headerResult.ok()) {
        LogsError("columnarShuffleParseArrowBatch ReadFileHeader failed: dataSize=%d msg=%s",
                  dataSize, headerResult.status().ToString().c_str());
        return -1;
    }
    auto& header = *headerResult;

    if (header.version != kArrowShuffleVersion) {
        LogsError("columnarShuffleParseArrowBatch version mismatch: got=%d expected=%d",
                  header.version, kArrowShuffleVersion);
        return -1;
    }
    if (header.layout != ShuffleLayout::COLUMNAR) {
        LogsError("columnarShuffleParseArrowBatch layout mismatch: expected COLUMNAR got=%d",
                  static_cast<int>(header.layout));
        return -1;
    }

    // Read the first (and for shuffle read, likely only) batch
    int64_t batchConsumed = 0;
    auto batchResult = ReadColumnarBatch(rawData + consumed,
                                         static_cast<int64_t>(dataSize) - consumed,
                                         header.schema, &batchConsumed);
    if (!batchResult.ok()) {
        LogsError("columnarShuffleParseArrowBatch ReadColumnarBatch failed: dataSize=%d consumed=%lld msg=%s",
                  dataSize, static_cast<long long>(consumed), batchResult.status().ToString().c_str());
        return -1;
    }
    auto& batch = *batchResult;

    int32_t vecCount = static_cast<int32_t>(header.schema.size());
    int32_t rowCount = batch.rowCount;

    // Create vectors and deserialize
    omniruntime::vec::BaseVector* vecs[vecCount]{};
    jint typeIdArrayElements[vecCount];
    jint precisionArrayElements[vecCount];
    jint scaleArrayElements[vecCount];
    jlong vecNativeIdArrayElements[vecCount];

    std::size_t bufIdx = 0;
    for (int32_t i = 0; i < vecCount; ++i) {
        const auto& desc = header.schema[i];
        typeIdArrayElements[i] = static_cast<jint>(desc.typeId);
        precisionArrayElements[i] = static_cast<jint>(desc.precision);
        scaleArrayElements[i] = static_cast<jint>(desc.scale);

        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(desc.typeId);
        if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
            auto dataType = DescriptorToOmniType(desc);
            vecs[i] = omniruntime::vec::VectorHelper::CreateComplexVector(dataType.get(), rowCount);
        } else {
            vecs[i] = omniruntime::vec::VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = reinterpret_cast<jlong>(vecs[i]);

        DeserializeArrowBufferToOmniVector(desc, rowCount, batch.buffers, bufIdx, vecs[i]);
    }

    return createResult(env, rowCount, vecCount, typeIdArrayElements,
                        precisionArrayElements, scaleArrayElements, vecNativeIdArrayElements);
}

// Arrow row batch parse — reads file header, reads first batch, deserializes to Omni vectors
int32_t DecompressionStream::rowShuffleParseArrowBatch(
    JNIEnv *env, const char* data, int32_t dataSize)
{
    const auto* rawData = reinterpret_cast<const uint8_t*>(data);

    // Read file header via RowShuffleParseInit (validates magic/version/layout==ROW)
    auto ctxResult = RowShuffleParseInit(rawData, static_cast<int64_t>(dataSize));
    if (!ctxResult.ok()) {
        LogsError("rowShuffleParseArrowBatch RowShuffleParseInit failed: dataSize=%d msg=%s",
                  dataSize, ctxResult.status().ToString().c_str());
        return -1;
    }
    auto ctx = std::move(*ctxResult);

    // Read next batch
    auto status = RowShuffleParseNextBatch(*ctx);
    if (!status.ok()) {
        LogsError("rowShuffleParseArrowBatch RowShuffleParseNextBatch failed: dataSize=%d msg=%s",
                  dataSize, status.ToString().c_str());
        return -1;
    }

    int32_t vecCount = ctx->vecCnt;
    int32_t rowCount = ctx->rowCnt;

    // Create vectors
    omniruntime::vec::BaseVector* vecs[vecCount]{};
    jint typeIdArrayElements[vecCount];
    jint precisionArrayElements[vecCount];
    jint scaleArrayElements[vecCount];
    jlong vecNativeIdArrayElements[vecCount];

    for (int32_t i = 0; i < vecCount; ++i) {
        const auto& desc = ctx->header.schema[i];
        typeIdArrayElements[i] = static_cast<jint>(desc.typeId);
        precisionArrayElements[i] = static_cast<jint>(desc.precision);
        scaleArrayElements[i] = static_cast<jint>(desc.scale);

        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(desc.typeId);
        if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
            auto dataType = DescriptorToOmniType(desc);
            vecs[i] = omniruntime::vec::VectorHelper::CreateComplexVector(dataType.get(), rowCount);
        } else {
            vecs[i] = omniruntime::vec::VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = reinterpret_cast<jlong>(vecs[i]);
    }

    // Parse rows using RowShuffleParseBatch
    RowShuffleParseBatch(*ctx, vecs);

    return createResult(env, rowCount, vecCount, typeIdArrayElements,
                        precisionArrayElements, scaleArrayElements, vecNativeIdArrayElements);
}

jobject ShuffleReaderDeserializer::getMetaInfo(JNIEnv *pEnv)
{
    return this->decompressionStream->result;
}

omniruntime::vec::VectorBatch* ShuffleReaderDeserializer::Next()
{
    AttachCurrentThreadAsDaemonOrThrow(vm_, &env);

    int32_t dataSize = this->decompressionStream->readSize(env);
    if (dataSize == -1 || dataSize == 0) {
        return nullptr;
    }

    auto uncompress = this->decompressionStream->decompress(env, dataSize);
    if (uncompress.first == nullptr || uncompress.second != dataSize) {
        LogsError("ShuffleReaderDeserializer::Next decompress failed: dataSize=%d decompressedSize=%d",
                  dataSize, uncompress.second);
        return nullptr;
    }

    int32_t rowCnt = 0;

    // Check for Arrow magic "OMSA" in the uncompressed data
    if (dataSize >= 4 && memcmp(uncompress.first, kArrowShuffleMagic, 4) == 0) {
        // Arrow path — dispatch by file header (layout self-describing)
        // Read layout from byte 5 (magic[4] + version[1] = offset 5)
        if (dataSize < 6) {
            LogsError("ShuffleReaderDeserializer::Next arrow data too short: dataSize=%d", dataSize);
            return nullptr;
        }
        uint8_t layoutByte = static_cast<uint8_t>(uncompress.first[5]);
        LogsInfo("ShuffleReaderDeserializer::Next ARROW path: dataSize=%d layoutByte=%d isRowShuffle=%d",
                 dataSize, layoutByte, static_cast<int>(this->isRowShuffle));
        if (layoutByte == static_cast<uint8_t>(ShuffleLayout::COLUMNAR)) {
            rowCnt = this->decompressionStream->columnarShuffleParseArrowBatch(
                env, uncompress.first, dataSize);
        } else if (layoutByte == static_cast<uint8_t>(ShuffleLayout::ROW)) {
            rowCnt = this->decompressionStream->rowShuffleParseArrowBatch(
                env, uncompress.first, dataSize);
        } else {
            LogsError("ShuffleReaderDeserializer::Next unknown arrow layout: layoutByte=%d dataSize=%d",
                      layoutByte, dataSize);
            return nullptr;
        }
    } else {
        // Arrow magic not found — data is not in Arrow format
        LogsError("ShuffleReaderDeserializer::Next Arrow magic 'OMSA' not found: dataSize=%d "
                  "isRowShuffle=%d", dataSize, static_cast<int>(this->isRowShuffle));
        return nullptr;
    }

    if (rowCnt == 0) {
        LogsError("ShuffleReaderDeserializer::Next parsed rowCnt=0, returning nullptr: dataSize=%d",
                  dataSize);
        return nullptr;
    }

    auto vectorBatch = new omniruntime::vec::VectorBatch(1);
    return vectorBatch;
}

}
