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

#ifndef SPARK_DECOMPRESSION_HH
#define SPARK_DECOMPRESSION_HH

#include "SparkFile.hh"
#include "jni.h"
#include "Common.hh"
#include "MemoryPool.hh"
#include "jni/jni_common.h"
#include "lz4.h"
#include "zlib.h"
#include "zstd.h"
#include "wrap/snappy_wrapper.h"

#include "vec_data.pb.h"

#include <cstddef>

namespace spark {

const int32_t BYTE_3_OFFSET = 24;
const int32_t BYTE_2_OFFSET = 16;
const int32_t BYTE_1_OFFSET = 8;
const int32_t BYTE_0_OFFSET = 0;

class DecompressionStream
{
public:
    int64_t shuffleCompressBlockSize;
    jobject dIn;
    jobject result;
    /** One VecBatch/ProtoRowBatch payload after readSize + decompress. */
    std::vector<char> uncompress;
    char* output = nullptr;

    DecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : dIn(dIn), shuffleCompressBlockSize(shuffleCompressBlockSize) {}

    virtual ~DecompressionStream()
    {
        delete[] output;
    }

    virtual std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) = 0;

    int32_t columnarShuffleParseBatch(JNIEnv *env, spark::VecBatch* vecBatch);

    int32_t rowShuffleParseBatch(JNIEnv *env, spark::ProtoRowBatch* protoRowBatch);

    int32_t mixedShuffleParseBatch(JNIEnv *env, spark::ProtoMixedBatch* protoMixedBatch);

    int32_t createResult(JNIEnv *env, int rowCount, int vecCount, jint* typeIdArrayElements, jint* precisionArrayElements,
        jint* scaleArrayElements, jlong* vecNativeIdArrayElements, jlong batchHandle = 0);

    std::pair<char*, int32_t> decompress(JNIEnv *pEnv, int32_t dataSize);

    int32_t readSize(JNIEnv *env);

    static bool IsMixedBatch(const char* buf, int32_t len);

protected:
    /** Sliding window: last readHeader payload (plain or decompressed); readSize/decompress consume from here. */
    std::vector<char> uncompressed;
    size_t uncompressedCursor_ = 0;
    size_t uncompressedLimit_ = 0;
    bool finishedReading_ = false;

    bool ensureBufferHasData(JNIEnv* env);
    bool consumeBytes(JNIEnv* env, void* dest, int32_t n);
    /**
     * Default: framed shuffle wire (3-byte header + chunk). UncompressionStream overrides.
     */
    virtual bool loadNextUncompressedChunk(JNIEnv* env) { return loadNextFramedFromWire(env); }
    bool loadNextFramedFromWire(JNIEnv* env);
};

class UncompressionStream final: public DecompressionStream {
public:
    UncompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override {
        return {nullptr, -1};
    }

    bool loadNextUncompressedChunk(JNIEnv* env) override;
};

class LZ4DecompressionStream final: public DecompressionStream {
public:
    LZ4DecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class SnappyDecompressionStream final: public DecompressionStream {
public:
    SnappyDecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class ZlibDecompressionStream final: public DecompressionStream {
public:
    ZlibDecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class ZstdDecompressionStream final: public DecompressionStream {
public:
    ZstdDecompressionStream(jobject dIn, int64_t shuffleCompressBlockSize)
        : DecompressionStream(dIn, shuffleCompressBlockSize) {}

protected:
    std::pair<char*, int32_t> doDecompression(char* input, int32_t inputLength) override;
};

class ShuffleReaderDeserializer final: public omniruntime::ColumnarBatchIterator {
public:
    ShuffleReaderDeserializer(JNIEnv* env, jobject jniIn, CompressionKind codec, int64_t shuffleCompressBlockSize, jboolean isRowShuffle, jboolean enableMix);

    ~ShuffleReaderDeserializer()
    {
        AttachCurrentThreadAsDaemonOrThrow(vm_, &env);
        env->DeleteGlobalRef(jniIn);
        vm_->DetachCurrentThread();
    };

    omniruntime::vec::VectorBatch* Next() override;
    jobject getMetaInfo(JNIEnv *env);
private:
    JavaVM* vm_;
    JNIEnv* env;
    jobject jniIn;
    int64_t shuffleCompressBlockSize;
    bool isRowShuffle;
    bool enableMix_;
    std::unique_ptr<DecompressionStream> decompressionStream;

    // 方案C：首批锁存流类型。canOutputMixed_ 在算子 Init 时一次算定（执行期不变），
    // 同 stage 的 map task 算子配置相同 → 一条读流内批次全混存或全非混存。
    // 仅首个批次扫描 mixType 锁定，后续批次 O(1) 直接分派。
    enum class StreamType { UNKNOWN, MIXED, NORMAL };
    StreamType streamType_ = StreamType::UNKNOWN;
};

}
#endif