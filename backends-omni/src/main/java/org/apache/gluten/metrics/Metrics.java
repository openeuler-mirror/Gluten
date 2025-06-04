/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.gluten.metrics;

import org.apache.gluten.exception.GlutenException;

/**
 * Metrics of Runtime.
 *
 * @since 2025-06-04
 */
public class Metrics implements IMetrics {
    /** number of input rows */
    private long[] inputRows;

    /** number of input vectors */
    private long[] inputVectors;

    /** number of input bytes */
    private long[] inputBytes;

    /** number of raw input rows */
    private long[] rawInputRows;

    /** number of raw input bytes */
    private long[] rawInputBytes;

    /** number of output rows */
    private long[] outputRows;

    /** number of output vectors */
    private long[] outputVectors;

    /** number of output bytes */
    private long[] outputBytes;

    /** cpu wall time cout */
    private long[] cpuCount;

    /** time of operator */
    private long[] wallNanos;

    /** scan time */
    private long[] scanTime;

    /** peak memory bytes */
    private long[] peakMemoryBytes;

    /** number of memory allocations */
    private long[] numMemoryAllocations;

    /** input bytes for spilling */
    private long[] spilledInputBytes;

    /** bytes written for spilling */
    private long[] spilledBytes;

    /** total rows written for spilling */
    private long[] spilledRows;

    /** total spilled partitions */
    private long[] spilledPartitions;

    /** total spilled files */
    private long[] spilledFiles;

    /** number of dynamic filters produced */
    private long[] numDynamicFiltersProduced;

    /** number of dynamic filters accepted */
    private long[] numDynamicFiltersAccepted;

    /** number of replaced with dynamic filter rows */
    private long[] numReplacedWithDynamicFilterRows;

    /** flush row count */
    private long[] flushRowCount;

    /** loaded to value hook */
    private long[] loadedToValueHook;

    /** number of skipped splits */
    private long[] skippedSplits;

    /** number of processed splits */
    private long[] processedSplits;

    /** number of skipped strides */
    private long[] skippedStrides;

    /** number of processed strides */
    private long[] processedStrides;

    /** remaining filter time */
    private long[] remainingFilterTime;

    /** io wait time */
    private long[] ioWaitTime;

    /** storage read bytes */
    private long[] storageReadBytes;

    /** local read bytes */
    private long[] localReadBytes;

    /** ram read bytes */
    private long[] ramReadBytes;

    /** preload splits */
    private long[] preloadSplits;

    /** number of physical written bytes */
    private long[] physicalWrittenBytes;

    /** write io time */
    private long[] writeIOTime;

    /** number of written files */
    private long[] numWrittenFiles;

    /** single metric */
    private SingleMetric singleMetric = new SingleMetric();

    /**
     * Create an instance for native metrics.
     *
     * @param inputRows number of input rows
     * @param inputVectors number of input vectors
     * @param inputBytes number of input bytes
     * @param rawInputRows number of raw input rows
     * @param rawInputBytes number of raw input bytes
     * @param outputRows number of output rows
     * @param outputVectors number of output vectors
     * @param outputBytes number of output bytes
     * @param cpuCount cpu wall time cout
     * @param wallNanos time of operator
     * @param omniToArrow omni to arrow
     * @param peakMemoryBytes peak memory bytes
     * @param numMemoryAllocations number of memory allocations
     * @param spilledInputBytes input bytes for spilling
     * @param spilledBytes bytes written for spilling
     * @param spilledRows total rows written for spilling
     * @param spilledPartitions total spilled partitions
     * @param spilledFiles total spilled files
     * @param numDynamicFiltersProduced number of dynamic filters produced
     * @param numDynamicFiltersAccepted number of dynamic filters accepted
     * @param numReplacedWithDynamicFilterRows number of replaced with dynamic filter rows
     * @param flushRowCount flush row count
     * @param loadedToValueHook loaded to value hook
     * @param scanTime scan time
     * @param skippedSplits number of skipped splits
     * @param processedSplits number of processed splits
     * @param skippedStrides number of skipped strides
     * @param processedStrides number of processed strides
     * @param remainingFilterTime remaining filter time
     * @param ioWaitTime io wait time
     * @param storageReadBytes storage read bytes
     * @param localReadBytes local read bytes
     * @param ramReadBytes ram read bytes
     * @param preloadSplits preload splits
     * @param physicalWrittenBytes number of physical written bytes
     * @param writeIOTime write io time
     * @param numWrittenFiles number of written files
     */
    public Metrics(
        long[] inputRows,
        long[] inputVectors,
        long[] inputBytes,
        long[] rawInputRows,
        long[] rawInputBytes,
        long[] outputRows,
        long[] outputVectors,
        long[] outputBytes,
        long[] cpuCount,
        long[] wallNanos,
        long omniToArrow,
        long[] peakMemoryBytes,
        long[] numMemoryAllocations,
        long[] spilledInputBytes,
        long[] spilledBytes,
        long[] spilledRows,
        long[] spilledPartitions,
        long[] spilledFiles,
        long[] numDynamicFiltersProduced,
        long[] numDynamicFiltersAccepted,
        long[] numReplacedWithDynamicFilterRows,
        long[] flushRowCount,
        long[] loadedToValueHook,
        long[] scanTime,
        long[] skippedSplits,
        long[] processedSplits,
        long[] skippedStrides,
        long[] processedStrides,
        long[] remainingFilterTime,
        long[] ioWaitTime,
        long[] storageReadBytes,
        long[] localReadBytes,
        long[] ramReadBytes,
        long[] preloadSplits,
        long[] physicalWrittenBytes,
        long[] writeIOTime,
        long[] numWrittenFiles) {
        this.inputRows = inputRows;
        this.inputVectors = inputVectors;
        this.inputBytes = inputBytes;
        this.rawInputRows = rawInputRows;
        this.rawInputBytes = rawInputBytes;
        this.outputRows = outputRows;
        this.outputVectors = outputVectors;
        this.outputBytes = outputBytes;
        this.cpuCount = cpuCount;
        this.wallNanos = wallNanos;
        this.scanTime = scanTime;
        this.singleMetric.setOmniToArrow(omniToArrow);
        this.peakMemoryBytes = peakMemoryBytes;
        this.numMemoryAllocations = numMemoryAllocations;
        this.spilledInputBytes = spilledInputBytes;
        this.spilledBytes = spilledBytes;
        this.spilledRows = spilledRows;
        this.spilledPartitions = spilledPartitions;
        this.spilledFiles = spilledFiles;
        this.numDynamicFiltersProduced = numDynamicFiltersProduced;
        this.numDynamicFiltersAccepted = numDynamicFiltersAccepted;
        this.numReplacedWithDynamicFilterRows = numReplacedWithDynamicFilterRows;
        this.flushRowCount = flushRowCount;
        this.loadedToValueHook = loadedToValueHook;
        this.skippedSplits = skippedSplits;
        this.processedSplits = processedSplits;
        this.skippedStrides = skippedStrides;
        this.processedStrides = processedStrides;
        this.remainingFilterTime = remainingFilterTime;
        this.ioWaitTime = ioWaitTime;
        this.storageReadBytes = storageReadBytes;
        this.localReadBytes = localReadBytes;
        this.ramReadBytes = ramReadBytes;
        this.preloadSplits = preloadSplits;
        this.physicalWrittenBytes = physicalWrittenBytes;
        this.writeIOTime = writeIOTime;
        this.numWrittenFiles = numWrittenFiles;
    }

    /**
     * generate OperatorMetrics
     *
     * @param index index of metrics
     * @return OperatorMetrics object
     */
    public OperatorMetrics genOperatorMetrics(int index) {
        if (index >= inputRows.length) {
            throw new GlutenException("Invalid index.");
        }

        return new OperatorMetrics(
            inputRows[index],
            inputVectors[index],
            inputBytes[index],
            rawInputRows[index],
            rawInputBytes[index],
            outputRows[index],
            outputVectors[index],
            outputBytes[index],
            cpuCount[index],
            wallNanos[index],
            peakMemoryBytes[index],
            numMemoryAllocations[index],
            spilledInputBytes[index],
            spilledBytes[index],
            spilledRows[index],
            spilledPartitions[index],
            spilledFiles[index],
            numDynamicFiltersProduced[index],
            numDynamicFiltersAccepted[index],
            numReplacedWithDynamicFilterRows[index],
            flushRowCount[index],
            loadedToValueHook[index],
            scanTime[index],
            skippedSplits[index],
            processedSplits[index],
            skippedStrides[index],
            processedStrides[index],
            remainingFilterTime[index],
            ioWaitTime[index],
            storageReadBytes[index],
            localReadBytes[index],
            ramReadBytes[index],
            preloadSplits[index],
            physicalWrittenBytes[index],
            writeIOTime[index],
            numWrittenFiles[index]
        );
    }

    /**
     * get SingleMetric
     *
     * @return SingleMetric object
     */
    public SingleMetric getSingleMetrics() {
        return singleMetric;
    }

    /**
     * class of SingleMetric
     */
    public static class SingleMetric {
        /** omni to arrow */
        private long omniToArrow;

        public long getOmniToArrow() {
            return omniToArrow;
        }

        public void setOmniToArrow(long omniToArrow) {
            this.omniToArrow = omniToArrow;
        }
    }
}