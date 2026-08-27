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
#include <vector/vector_common.h>
#include "shuffle/splitter.h"
#include <cstdint>

namespace spark {

namespace {
bool readVarint(const uint8_t* buf, int32_t len, size_t& pos, uint64_t& out)
{
    out = 0;
    int shift = 0;
    while (pos < static_cast<size_t>(len)) {
        const uint8_t b = buf[pos++];
        out |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) {
            return true;
        }
        shift += 7;
        if (shift >= 64) {
            return false;
        }
    }
    return false;
}
}

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
    CompressionKind codec, int64_t shuffleCompressBlockSize, jboolean isRowShuffle, jboolean enableMix)
: env(env), shuffleCompressBlockSize(shuffleCompressBlockSize), isRowShuffle(JNI_TRUE == isRowShuffle), enableMix_(JNI_TRUE == enableMix)
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

int32_t DecompressionStream::columnarShuffleParseBatch(JNIEnv *env, spark::VecBatch* vecBatch)
{
    int32_t vecCount = vecBatch->veccnt();
    int32_t rowCount = vecBatch->rowcnt();
    // convert vecBatch into an omni array of vec values as the final result
    omniruntime::vec::BaseVector* vecs[vecCount]{};

    jint typeIdArrayElements[vecCount];
    jint precisionArrayElements[vecCount];
    jint scaleArrayElements[vecCount];
    jlong vecNativeIdArrayElements[vecCount];

    for (auto i = 0; i < vecCount; ++i) {
        const spark::Vec& protoVec = vecBatch->vecs(i);
        const spark::VecType& protoType = protoVec.vectype();
        scaleArrayElements[i] = protoType.scale();
        precisionArrayElements[i] = protoType.precision();
        typeIdArrayElements[i] = static_cast<jint>(protoType.typeid_());

        // create native vector
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoType.typeid_());

        if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
            auto dataType = Splitter::ProtoTypeToOmniType(protoType);
            vecs[i] = VectorHelper::CreateComplexVector(dataType.get(), rowCount);
        } else {
            vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = (jlong)(vecs[i]);

        Splitter::DeserializeProtoVecToOmniVector(protoVec, vecs[i]);
    }

    return createResult(env, rowCount, vecCount, typeIdArrayElements,
        precisionArrayElements, scaleArrayElements, vecNativeIdArrayElements);
}

int32_t DecompressionStream::rowShuffleParseBatch(JNIEnv *env, spark::ProtoRowBatch* protoRowBatch)
{
    int32_t vecCount = protoRowBatch->veccnt();
    int32_t rowCount = protoRowBatch->rowcnt();
    omniruntime::vec::BaseVector* vecs[vecCount];
    std::vector<omniruntime::type::DataTypeId> omniDataTypeIds(vecCount);

    jint typeIdArrayElements[vecCount];
    jint precisionArrayElements[vecCount];
    jint scaleArrayElements[vecCount];
    jlong vecNativeIdArrayElements[vecCount];

    for (auto i = 0; i < vecCount; ++i) {
        const spark::VecType& protoTypeId = protoRowBatch->vectypes(i);
        scaleArrayElements[i] = protoTypeId.scale();
        precisionArrayElements[i] = protoTypeId.precision();
        typeIdArrayElements[i] = static_cast<jint>(protoTypeId.typeid_());
        omniDataTypeIds[i] = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());

        // create native vector
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());
        if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
            auto dataType = Splitter::ProtoTypeToOmniType(protoTypeId);
            vecs[i] = VectorHelper::CreateComplexVector(dataType.get(), rowCount);
        } else {
            vecs[i] = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
        }
        vecNativeIdArrayElements[i] = (jlong)(vecs[i]);
    }

    std::unique_ptr<RowParser> parser = std::make_unique<RowParser>(omniDataTypeIds);
    char *rows = const_cast<char*>(protoRowBatch->rows().data());
    const int32_t *offsets = reinterpret_cast<const int32_t*>(protoRowBatch->offsets().data());
    for (auto i = 0; i < rowCount; ++i) {
        char *rowPtr = rows + offsets[i];
        parser->ParseOneRow(reinterpret_cast<uint8_t*>(rowPtr), vecs, i);
    }

    return createResult(env, rowCount, vecCount, typeIdArrayElements,
        precisionArrayElements, scaleArrayElements, vecNativeIdArrayElements);
}

int32_t DecompressionStream::mixedShuffleParseBatch(JNIEnv *env, spark::ProtoMixedBatch* protoMixedBatch)
{
    int32_t columnarVecCount = protoMixedBatch->veccnt();
    int32_t rowCount = protoMixedBatch->rowcnt();
    int32_t rowVecTypeCount = protoMixedBatch->vectypes_size();
    
    std::vector<omniruntime::type::DataTypeId> rowOmniDataTypeIds;
    rowOmniDataTypeIds.reserve(rowVecTypeCount);
    
    for (int32_t i = 0; i < rowVecTypeCount; ++i) {
        const spark::VecType& protoTypeId = protoMixedBatch->vectypes(i);
        auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoTypeId.typeid_());
        rowOmniDataTypeIds.push_back(vectorDataTypeId);
    }
    
    auto *mixedBatch = new omniruntime::vec::MixedVectorBatch(rowCount, rowOmniDataTypeIds);
    
    std::vector<omniruntime::vec::BaseVector*> columnarVecs;
    columnarVecs.reserve(columnarVecCount);
    
    std::vector<jint> typeIdArrayElements(columnarVecCount);
    std::vector<jint> precisionArrayElements(columnarVecCount);
    std::vector<jint> scaleArrayElements(columnarVecCount);
    std::vector<jlong> vecNativeIdArrayElements(columnarVecCount);
    
    if (protoMixedBatch->vecs_size() > 0) {
        for (int32_t i = 0; i < columnarVecCount; ++i) {
            const spark::Vec& protoVec = protoMixedBatch->vecs(i);
            const spark::VecType& protoType = protoVec.vectype();
            
            typeIdArrayElements[i] = static_cast<jint>(protoType.typeid_());
            precisionArrayElements[i] = protoType.precision();
            scaleArrayElements[i] = protoType.scale();
            
            auto vectorDataTypeId = static_cast<omniruntime::type::DataTypeId>(protoType.typeid_());
            omniruntime::vec::BaseVector* vec = nullptr;
            
            if (vectorDataTypeId == OMNI_ARRAY || vectorDataTypeId == OMNI_MAP || vectorDataTypeId == OMNI_ROW) {
                auto dataType = Splitter::ProtoTypeToOmniType(protoType);
                vec = VectorHelper::CreateComplexVector(dataType.get(), rowCount);
            } else {
                vec = VectorHelper::CreateVector(OMNI_FLAT, vectorDataTypeId, rowCount);
            }
            
            Splitter::DeserializeProtoVecToOmniVector(protoVec, vec);
            columnarVecs.push_back(vec);
            mixedBatch->Append(vec);
            vecNativeIdArrayElements[i] = reinterpret_cast<jlong>(vec);
        }
    }
    
    char *rows = const_cast<char*>(protoMixedBatch->rows().data());
    const int32_t *offsets = reinterpret_cast<const int32_t*>(protoMixedBatch->offsets().data());
    
    const int32_t *keyLengths = nullptr;
    const int32_t *stateOffsets = nullptr;
    bool hasMetadata = (protoMixedBatch->key_lengths().size() > 0);
    
    if (hasMetadata) {
        keyLengths = reinterpret_cast<const int32_t*>(protoMixedBatch->key_lengths().data());
        stateOffsets = reinterpret_cast<const int32_t*>(protoMixedBatch->state_offsets().data());
    }
    
    size_t totalBufferSize = protoMixedBatch->rows().size();
    
    mixedBatch->PrepareRowArena(totalBufferSize);
    auto *arenaAllocator = mixedBatch->GetRowArena();
    
    for (int32_t i = 0; i < rowCount; ++i) {
        int32_t rowLength = (i < rowCount - 1) ? 
            (offsets[i + 1] - offsets[i]) : 
            (static_cast<int32_t>(protoMixedBatch->rows().size()) - offsets[i]);
        char *rowPtr = rows + offsets[i];
        
        const uint8_t *start = nullptr;
        auto *pos = arenaAllocator->AllocateContinue(rowLength, start);
        memcpy(pos, rowPtr, rowLength);
        
        if (hasMetadata) {
            mixedBatch->SetArenaRow(i, start, keyLengths[i], stateOffsets[i], rowLength);
        } else {
            mixedBatch->SetArenaRow(i, start, rowLength);
        }
    }
    
    mixedBatch->SetMode(protoMixedBatch->vecs_size() == 0 ? 
        omniruntime::vec::COMPLETE_ROW_ONLY : 
        omniruntime::vec::HYBRID_ROW_COLUMN);
    
    return createResult(env, rowCount, columnarVecCount, typeIdArrayElements.data(),
        precisionArrayElements.data(), scaleArrayElements.data(), vecNativeIdArrayElements.data(),
        reinterpret_cast<jlong>(mixedBatch));
}

int32_t DecompressionStream::createResult(JNIEnv *env, int rowCount, int vecCount,
    jint* typeIdArrayElements, jint* precisionArrayElements,
    jint* scaleArrayElements, jlong* vecNativeIdArrayElements, jlong batchHandle)
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

    env->SetObjectField(result, fidTypeIds, typeIdsArr);
    env->SetObjectField(result, fidPrec,    precArr);
    env->SetObjectField(result, fidScales,  scaleArr);
    env->SetObjectField(result, fidVecIds,  vecIdArr);
    env->SetIntField(result, fidRowCount, rowCount);
    env->SetIntField(result, fidVecCount, vecCount);
    
    if (batchHandle != 0) {
        env->SetLongField(result, fidBatchHandle, batchHandle);
    }

    return rowCount;
}

bool DecompressionStream::IsMixedBatch(const char* buf, int32_t len)
{
    if (buf == nullptr || len <= 0) {
        return false;
    }
    const uint8_t* p = reinterpret_cast<const uint8_t*>(buf);
    size_t pos = 0;
    const size_t total = static_cast<size_t>(len);
    while (pos < total) {
        uint64_t tag = 0;
        if (!readVarint(p, len, pos, tag)) {
            return false;
        }
        const uint32_t fieldNumber = static_cast<uint32_t>(tag >> 3);
        const uint32_t wireType = static_cast<uint32_t>(tag & 0x07);
        if (wireType == 0) {
            uint64_t value = 0;
            if (!readVarint(p, len, pos, value)) {
                return false;
            }
            if (fieldNumber == 7) {
                return value != 0;
            }
        } else if (wireType == 1) {
            if (pos + 8 > total) return false;
            pos += 8;
        } else if (wireType == 2) {
            uint64_t length = 0;
            if (!readVarint(p, len, pos, length)) {
                return false;
            }
            if (length > static_cast<uint64_t>(total - pos)) {
                return false;
            }
            pos += static_cast<size_t>(length);
        } else if (wireType == 5) {
            if (pos + 4 > total) return false;
            pos += 4;
        } else {
            return false;
        }
    }
    return false;
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
        return nullptr;
    }

    // 首批锁存（方案C）：流内批次全混存或全非混存（canOutputMixed_ 执行期不变），
    // 只扫首个批次锁定类型；enableMix_ 关闭时连首扫都跳过（锁定 NORMAL）。
    if (streamType_ == StreamType::UNKNOWN) {
        streamType_ = (this->enableMix_ &&
                       DecompressionStream::IsMixedBatch(uncompress.first, uncompress.second))
            ? StreamType::MIXED
            : StreamType::NORMAL;
    }

    int32_t rowCnt = 0;
    if (streamType_ == StreamType::MIXED) {
        auto *protoMixedBatch = new spark::ProtoMixedBatch();
        protoMixedBatch->ParseFromArray(uncompress.first, uncompress.second);
        rowCnt = this->decompressionStream->mixedShuffleParseBatch(env, protoMixedBatch);
        delete protoMixedBatch;
    } else if (this->isRowShuffle) {
        auto *protoRowBatch = new spark::ProtoRowBatch();
        protoRowBatch->ParseFromArray(uncompress.first, uncompress.second);
        rowCnt = this->decompressionStream->rowShuffleParseBatch(env, protoRowBatch);
        delete protoRowBatch;
    } else {
        auto *vecBatch = new spark::VecBatch();
        vecBatch->ParseFromArray(uncompress.first, uncompress.second);
        rowCnt = this->decompressionStream->columnarShuffleParseBatch(env, vecBatch);
        delete vecBatch;
    }

    if (rowCnt == 0) {
        return nullptr;
    }

    auto vectorBatch = new omniruntime::vec::VectorBatch(1);
    return vectorBatch;
}

}
