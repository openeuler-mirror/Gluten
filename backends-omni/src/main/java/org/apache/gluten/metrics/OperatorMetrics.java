/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */

package org.apache.gluten.metrics;

/**
 * Operator Metrics of Runtime.
 *
 * @since 2025-06-04
 */
public class OperatorMetrics implements IOperatorMetrics {
    /** number of input rows */
    private long inputRows;

    /** number of input vectors */
    private long inputVectors;

    /** number of input bytes */
    private long inputBytes;

    /** number of raw input rows */
    private long rawInputRows;

    /** number of raw input bytes */
    private long rawInputBytes;

    /** number of output rows */
    private long outputRows;

    /** number of output vectors */
    private long outputVectors;

    /** number of output bytes */
    private long outputBytes;

    /** cpu wall time cout */
    private long cpuCount;

    /** time of operator */
    private long wallNanos;

    /** scan time */
    private long scanTime;

    /** peak memory bytes */
    private long peakMemoryBytes;

    /** number of memory allocations */
    private long numMemoryAllocations;

    /** input bytes for spilling */
    private long spilledInputBytes;

    /** bytes written for spilling */
    private long spilledBytes;

    /** total rows written for spilling */
    private long spilledRows;

    /** total spilled partitions */
    private long spilledPartitions;

    /** total spilled files */
    private long spilledFiles;

    /** number of dynamic filters produced */
    private long numDynamicFiltersProduced;

    /** number of dynamic filters accepted */
    private long numDynamicFiltersAccepted;

    /** number of replaced with dynamic filter rows */
    private long numReplacedWithDynamicFilterRows;

    /** flush row count */
    private long flushRowCount;

    /** loaded to value hook */
    private long loadedToValueHook;

    /** number of skipped splits */
    private long skippedSplits;

    /** number of processed splits */
    private long processedSplits;

    /** number of skipped strides */
    private long skippedStrides;

    /** number of processed strides */
    private long processedStrides;

    /** remaining filter time */
    private long remainingFilterTime;

    /** io wait time */
    private long ioWaitTime;

    /** storage read bytes */
    private long storageReadBytes;

    /** local read bytes */
    private long localReadBytes;

    /** ram read bytes */
    private long ramReadBytes;

    /** preload splits */
    private long preloadSplits;

    /** number of physical written bytes */
    private long physicalWrittenBytes;

    /** write io time */
    private long writeIOTime;

    /** number of written files */
    private long numWrittenFiles;

    /**
     * Create an instance for operator metrics.
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
    public OperatorMetrics(
        long inputRows,
        long inputVectors,
        long inputBytes,
        long rawInputRows,
        long rawInputBytes,
        long outputRows,
        long outputVectors,
        long outputBytes,
        long cpuCount,
        long wallNanos,
        long peakMemoryBytes,
        long numMemoryAllocations,
        long spilledInputBytes,
        long spilledBytes,
        long spilledRows,
        long spilledPartitions,
        long spilledFiles,
        long numDynamicFiltersProduced,
        long numDynamicFiltersAccepted,
        long numReplacedWithDynamicFilterRows,
        long flushRowCount,
        long loadedToValueHook,
        long scanTime,
        long skippedSplits,
        long processedSplits,
        long skippedStrides,
        long processedStrides,
        long remainingFilterTime,
        long ioWaitTime,
        long storageReadBytes,
        long localReadBytes,
        long ramReadBytes,
        long preloadSplits,
        long physicalWrittenBytes,
        long writeIOTime,
        long numWrittenFiles) {
        this.inputRows = inputRows;
        this.inputVectors = inputVectors;
        this.inputBytes = inputBytes;
        this.rawInputBytes = rawInputBytes;
        this.outputRows = outputRows;
        this.outputVectors = outputVectors;
        this.outputBytes = outputBytes;
        this.cpuCount = cpuCount;
        this.wallNanos = wallNanos;
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

    public long getInputRows() {
        return inputRows;
    }

    public void setInputRows(long inputRows) {
        this.inputRows = inputRows;
    }

    public long getInputVectors() {
        return inputVectors;
    }

    public void setInputVectors(long inputVectors) {
        this.inputVectors = inputVectors;
    }

    public long getInputBytes() {
        return inputBytes;
    }

    public void setInputBytes(long inputBytes) {
        this.inputBytes = inputBytes;
    }

    public long getRawInputRows() {
        return rawInputRows;
    }

    public void setRawInputRows(long rawInputRows) {
        this.rawInputRows = rawInputRows;
    }

    public long getRawInputBytes() {
        return rawInputBytes;
    }

    public void setRawInputBytes(long rawInputBytes) {
        this.rawInputBytes = rawInputBytes;
    }

    public long getOutputRows() {
        return outputRows;
    }

    public void setOutputRows(long outputRows) {
        this.outputRows = outputRows;
    }

    public long getOutputVectors() {
        return outputVectors;
    }

    public void setOutputVectors(long outputVectors) {
        this.outputVectors = outputVectors;
    }

    public long getOutputBytes() {
        return outputBytes;
    }

    public void setOutputBytes(long outputBytes) {
        this.outputBytes = outputBytes;
    }

    public long getCpuCount() {
        return cpuCount;
    }

    public void setCpuCount(long cpuCount) {
        this.cpuCount = cpuCount;
    }

    public long getWallNanos() {
        return wallNanos;
    }

    public void setWallNanos(long wallNanos) {
        this.wallNanos = wallNanos;
    }

    public long getPeakMemoryBytes() {
        return peakMemoryBytes;
    }

    public void setPeakMemoryBytes(long peakMemoryBytes) {
        this.peakMemoryBytes = peakMemoryBytes;
    }

    public long getNumMemoryAllocations() {
        return numMemoryAllocations;
    }

    public void setNumMemoryAllocations(long numMemoryAllocations) {
        this.numMemoryAllocations = numMemoryAllocations;
    }

    public long getSpilledInputBytes() {
        return spilledInputBytes;
    }

    public void setSpilledInputBytes(long spilledInputBytes) {
        this.spilledInputBytes = spilledInputBytes;
    }

    public long getSpilledBytes() {
        return spilledBytes;
    }

    public void setSpilledBytes(long spilledBytes) {
        this.spilledBytes = spilledBytes;
    }

    public long getSpilledRows() {
        return spilledRows;
    }

    public void setSpilledRows(long spilledRows) {
        this.spilledRows = spilledRows;
    }

    public long getSpilledPartitions() {
        return spilledPartitions;
    }

    public void setSpilledPartitions(long spilledPartitions) {
        this.spilledPartitions = spilledPartitions;
    }

    public long getSpilledFiles() {
        return spilledFiles;
    }

    public void setSpilledFiles(long spilledFiles) {
        this.spilledFiles = spilledFiles;
    }

    public long getNumDynamicFiltersProduced() {
        return numDynamicFiltersProduced;
    }

    public void setNumDynamicFiltersProduced(long numDynamicFiltersProduced) {
        this.numDynamicFiltersProduced = numDynamicFiltersProduced;
    }

    public long getNumDynamicFiltersAccepted() {
        return numDynamicFiltersAccepted;
    }

    public void setNumDynamicFiltersAccepted(long numDynamicFiltersAccepted) {
        this.numDynamicFiltersAccepted = numDynamicFiltersAccepted;
    }

    public long getNumReplacedWithDynamicFilterRows() {
        return numReplacedWithDynamicFilterRows;
    }

    public void setNumReplacedWithDynamicFilterRows(long numReplacedWithDynamicFilterRows) {
        this.numReplacedWithDynamicFilterRows = numReplacedWithDynamicFilterRows;
    }

    public long getFlushRowCount() {
        return flushRowCount;
    }

    public void setFlushRowCount(long flushRowCount) {
        this.flushRowCount = flushRowCount;
    }

    public long getLoadedToValueHook() {
        return loadedToValueHook;
    }

    public void setLoadedToValueHook(long loadedToValueHook) {
        this.loadedToValueHook = loadedToValueHook;
    }

    public long getScanTime() {
        return scanTime;
    }

    public void setScanTime(long scanTime) {
        this.scanTime = scanTime;
    }

    public long getSkippedSplits() {
        return skippedSplits;
    }

    public void setSkippedSplits(long skippedSplits) {
        this.skippedSplits = skippedSplits;
    }

    public long getProcessedSplits() {
        return processedSplits;
    }

    public void setProcessedSplits(long processedSplits) {
        this.processedSplits = processedSplits;
    }

    public long getSkippedStrides() {
        return skippedStrides;
    }

    public void setSkippedStrides(long skippedStrides) {
        this.skippedStrides = skippedStrides;
    }

    public long getProcessedStrides() {
        return processedStrides;
    }

    public void setProcessedStrides(long processedStrides) {
        this.processedStrides = processedStrides;
    }

    public long getRemainingFilterTime() {
        return remainingFilterTime;
    }

    public void setRemainingFilterTime(long remainingFilterTime) {
        this.remainingFilterTime = remainingFilterTime;
    }

    public long getIoWaitTime() {
        return ioWaitTime;
    }

    public void setIoWaitTime(long ioWaitTime) {
        this.ioWaitTime = ioWaitTime;
    }

    public long getStorageReadBytes() {
        return storageReadBytes;
    }

    public void setStorageReadBytes(long storageReadBytes) {
        this.storageReadBytes = storageReadBytes;
    }

    public long getLocalReadBytes() {
        return localReadBytes;
    }

    public void setLocalReadBytes(long localReadBytes) {
        this.localReadBytes = localReadBytes;
    }

    public long getRamReadBytes() {
        return ramReadBytes;
    }

    public void setRamReadBytes(long ramReadBytes) {
        this.ramReadBytes = ramReadBytes;
    }

    public long getPreloadSplits() {
        return preloadSplits;
    }

    public void setPreloadSplits(long preloadSplits) {
        this.preloadSplits = preloadSplits;
    }

    public long getPhysicalWrittenBytes() {
        return physicalWrittenBytes;
    }

    public void setPhysicalWrittenBytes(long physicalWrittenBytes) {
        this.physicalWrittenBytes = physicalWrittenBytes;
    }

    public long getWriteIOTime() {
        return writeIOTime;
    }

    public void setWriteIOTime(long writeIOTime) {
        this.writeIOTime = writeIOTime;
    }

    public long getNumWrittenFiles() {
        return numWrittenFiles;
    }

    public void setNumWrittenFiles(long numWrittenFiles) {
        this.numWrittenFiles = numWrittenFiles;
    }
}