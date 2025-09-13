/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

#pragma once

#include "compute/ProtobufUtils.h"
#include "config.pb.h"

namespace omniruntime {
// store configurations that are general to all backend types
const std::string kDebugModeEnabled = "spark.gluten.sql.debug";

const std::string kGlutenSaveDir = "spark.gluten.saveDir";

const std::string kCaseSensitive = "spark.sql.caseSensitive";

const std::string kSessionTimezone = "spark.sql.session.timeZone";

const std::string kAllowPrecisionLoss = "spark.sql.decimalOperations.allowPrecisionLoss";

const std::string kIgnoreMissingFiles = "spark.sql.files.ignoreMissingFiles";

const std::string kDefaultSessionTimezone = "spark.gluten.sql.session.timeZone.default";

const std::string kSparkOverheadMemory = "spark.gluten.memoryOverhead.size.in.bytes";

const std::string kSparkOffHeapMemory = "spark.gluten.memory.offHeap.size.in.bytes";

const std::string kSparkTaskOffHeapMemory = "spark.gluten.memory.task.offHeap.size.in.bytes";

const std::string kMemoryReservationBlockSize = "spark.gluten.memory.reservationBlockSize";
const uint64_t kMemoryReservationBlockSizeDefault = 8 << 20;

const std::string kSparkBatchSize = "spark.gluten.sql.columnar.maxBatchSize";

const std::string kParquetBlockSize = "parquet.block.size";

const std::string kParquetBlockRows = "parquet.block.rows";

const std::string kParquetGzipWindowSize = "parquet.gzip.windowSize";
const std::string kGzipWindowSize4k = "4096";

const std::string kParquetCompressionCodec = "spark.sql.parquet.compression.codec";

const std::string kColumnarToRowMemoryThreshold = "spark.gluten.sql.columnarToRowMemoryThreshold";

const std::string kUGIUserName = "spark.gluten.ugi.username";
const std::string kUGITokens = "spark.gluten.ugi.tokens";

const std::string kShuffleCompressionCodec = "spark.gluten.sql.columnar.shuffle.codec";
const std::string kShuffleCompressionCodecBackend = "spark.gluten.sql.columnar.shuffle.codecBackend";
const std::string kShuffleSpillDiskWriteBufferSize = "spark.shuffle.spill.diskWriteBufferSize";
const std::string kQatBackendName = "qat";
const std::string kIaaBackendName = "iaa";

const std::string kSparkRedactionRegex = "spark.redaction.regex";
const std::string kSparkRedactionString = "*********(redacted)";

const std::string kSparkLegacyTimeParserPolicy = "spark.sql.legacy.timeParserPolicy";
const std::string kShuffleFileBufferSize = "spark.shuffle.file.buffer";

// memory
const std::string kSpillStrategy = "spark.gluten.sql.columnar.backend.omni.spillStrategy";
const std::string kSpillStrategyDefaultValue = "auto";
const std::string kSpillThreadNum = "spark.gluten.sql.columnar.backend.omni.spillThreadNum";
const uint32_t kSpillThreadNumDefaultValue = 0;
const std::string kMemFraction = "spark.gluten.sql.columnar.backend.omni.memFraction";
const std::string kAggregationSpillEnabled = "spark.gluten.sql.columnar.backend.omni.aggregationSpillEnabled";
const std::string kJoinSpillEnabled = "spark.gluten.sql.columnar.backend.omni.joinSpillEnabled";
const std::string kOrderBySpillEnabled = "spark.gluten.sql.columnar.backend.omni.orderBySpillEnabled";

// spill config
const std::string kMaxSpillLevel = "spark.gluten.sql.columnar.backend.omni.maxSpillLevel";
const std::string kMaxSpillFileSize = "spark.gluten.sql.columnar.backend.omni.maxSpillFileSize";
const std::string kSpillStartPartitionBit = "spark.gluten.sql.columnar.backend.omni.spillStartPartitionBit";
const std::string kSpillPartitionBits = "spark.gluten.sql.columnar.backend.omni.spillPartitionBits";
const std::string kMaxSpillRunRows = "spark.gluten.sql.columnar.backend.omni.MaxSpillRunRows";
const std::string kMaxSpillBytes = "spark.gluten.sql.columnar.backend.omni.MaxSpillBytes";
const std::string kSpillReadBufferSize = "spark.unsafe.sorter.spill.reader.buffer.size";
const uint64_t kMaxSpillFileSizeDefault = 1L * 1024 * 1024 * 1024;

const std::string kSpillableReservationGrowthPct =
    "spark.gluten.sql.columnar.backend.omni.spillableReservationGrowthPct";
const std::string kSpillPrefixSortEnabled = "spark.gluten.sql.columnar.backend.omni.spillPrefixsortEnabled";
// Whether to compress data spilled. Compression will use spark.io.compression.codec or kSpillCompressionKind.
const std::string kSparkShuffleSpillCompress = "spark.shuffle.spill.compress";
const std::string kCompressionKind = "spark.io.compression.codec";
/// The compression codec to use for spilling. Use kCompressionKind if not set.
const std::string kSpillCompressionKind = "spark.gluten.sql.columnar.backend.omni.spillCompressionCodec";
const std::string kMaxPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.omni.maxPartialAggregationMemoryRatio";
const std::string kMaxExtendedPartialAggregationMemoryRatio =
    "spark.gluten.sql.columnar.backend.omni.maxExtendedPartialAggregationMemoryRatio";
const std::string kAbandonPartialAggregationMinPct =
    "spark.gluten.sql.columnar.backend.omni.abandonPartialAggregationMinPct";
const std::string kAbandonPartialAggregationMinRows =
    "spark.gluten.sql.columnar.backend.omni.abandonPartialAggregationMinRows";

// execution
const std::string kBloomFilterExpectedNumItems = "spark.gluten.sql.columnar.backend.omni.bloomFilter.expectedNumItems";
const std::string kBloomFilterNumBits = "spark.gluten.sql.columnar.backend.omni.bloomFilter.numBits";
const std::string kBloomFilterMaxNumBits = "spark.gluten.sql.columnar.backend.omni.bloomFilter.maxNumBits";

const std::string kShowTaskMetricsWhenFinished = "spark.gluten.sql.columnar.backend.omni.showTaskMetricsWhenFinished";
const bool kShowTaskMetricsWhenFinishedDefault = false;

const std::string kEnableUserExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.omni.enableUserExceptionStacktrace";
const bool kEnableUserExceptionStacktraceDefault = true;

const std::string kEnableSystemExceptionStacktrace =
    "spark.gluten.sql.columnar.backend.omni.enableSystemExceptionStacktrace";
const bool kEnableSystemExceptionStacktraceDefault = true;

const std::string kMemoryUseHugePages = "spark.gluten.sql.columnar.backend.omni.memoryUseHugePages";
const bool kMemoryUseHugePagesDefault = false;

const std::string kHiveConnectorId = "test-hive";

// memory cache
constexpr int64_t kMaxMemory = std::numeric_limits<int64_t>::max();

/* configs for file read in omni */
const std::string kDirectorySizeGuess = "spark.gluten.sql.columnar.backend.omni.directorySizeGuess";
const std::string kFilePreloadThreshold = "spark.gluten.sql.columnar.backend.omni.filePreloadThreshold";
const std::string kPrefetchRowGroups = "spark.gluten.sql.columnar.backend.omni.prefetchRowGroups";
const std::string kLoadQuantum = "spark.gluten.sql.columnar.backend.omni.loadQuantum";
const std::string kMaxCoalescedDistance = "spark.gluten.sql.columnar.backend.omni.maxCoalescedDistance";
const std::string kMaxCoalescedBytes = "spark.gluten.sql.columnar.backend.omni.maxCoalescedBytes";
const std::string kCachePrefetchMinPct = "spark.gluten.sql.columnar.backend.omni.cachePrefetchMinPct";

// write fies
const std::string kMaxPartitions = "spark.gluten.sql.columnar.backend.omni.maxPartitionsPerWritersSession";

const std::string kGlogVerboseLevel = "spark.gluten.sql.columnar.backend.omni.glogVerboseLevel";
const uint32_t kGlogVerboseLevelDefault = 0;
const uint32_t kGlogVerboseLevelMaximum = 99;
const std::string kGlogSeverityLevel = "spark.gluten.sql.columnar.backend.omni.glogSeverityLevel";
const uint32_t kGlogSeverityLevelDefault = 1;
const std::string KSpillDir = "spill_dir";

// metrics
const std::string kDynamicFiltersProduced = "dynamicFiltersProduced";
const std::string kDynamicFiltersAccepted = "dynamicFiltersAccepted";
const std::string kReplacedWithDynamicFilterRows = "replacedWithDynamicFilterRows";
const std::string kFlushRowCount = "flushRowCount";
const std::string kLoadedToValueHook = "loadedToValueHook";
const std::string kTotalScanTime = "totalScanTime";
const std::string kSkippedSplits = "skippedSplits";
const std::string kProcessedSplits = "processedSplits";
const std::string kSkippedStrides = "skippedStrides";
const std::string kProcessedStrides = "processedStrides";
const std::string kRemainingFilterTime = "totalRemainingFilterTime";
const std::string kIoWaitTime = "ioWaitWallNanos";
const std::string kStorageReadBytes = "storageReadBytes";
const std::string kLocalReadBytes = "localReadBytes";
const std::string kRamReadBytes = "ramReadBytes";
const std::string kPreloadSplits = "readyPreloadedSplits";
const std::string kNumWrittenFiles = "numWrittenFiles";
const std::string kWriteIOTime = "writeIOTime";

// spill config
const std::string KSpillHashAggRowThreshold = "spark.gluten.sql.columnar.backend.omni.hashAggSpill.rowThreshold";
const std::string KSpillSortRowThreshold = "spark.gluten.sql.columnar.backend.omni.sortSpill.rowThreshold";

const std::string KColumnarSpillMemThreshold = "spark.gluten.sql.columnar.backend.omni.spill.memFraction";
const std::string KColumnarSpillWriteBufferSize = "spark.gluten.sql.columnar.backend.omni.spill.writeBufferSize";
const std::string KColumnarSpillDirDiskReserveSize = "spark.gluten.sql.columnar.backend.omni.spill.dirDiskReserveSize";

// others
const std::string kHiveDefaultPartition = "__HIVE_DEFAULT_PARTITION__";

inline std::unordered_map<std::string, std::string> ParseConfMap(const uint8_t *planData, const int32_t planDataLength)
{
    std::unordered_map<std::string, std::string> sparkConfs;
    gluten::ConfigMap pConfigMap;
    ParseProtobuf(planData, planDataLength, &pConfigMap);
    for (const auto &pair : pConfigMap.configs()) {
        sparkConfs.emplace(pair.first, pair.second);
    }
    return sparkConfs;
}
} // namespace gluten
