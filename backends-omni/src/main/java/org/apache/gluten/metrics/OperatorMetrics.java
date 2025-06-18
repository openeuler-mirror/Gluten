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
    private long numInputVecBatches;

    /** number of input bytes */
    private long inputBytes;

    /** number of input time */
    private long addInputTime;

    /** number of raw input rows */
    private long rawInputRows;

    /** number of raw input bytes */
    private long rawInputBytes;

    /** number of output rows */
    private long outputRows;

    /** number of output vectors */
    private long numOutputVecBatches;

    /** number of output bytes */
    private long outputBytes;

    /** number of output time */
    private long getOutputTime;

    /** cpu wall time cout */
    private long cpuCount;

    /** time of operator */
    private long cpuNanos;

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

    // For BHJ/SHJ
    private long buildInputRows;
    private long buildNumInputVecBatches;
    private long buildAddInputTime;
    private long buildGetOutputTime;

    private long lookupInputRows;
    private long lookupNumInputVecBatches;
    private long lookupOutputRows;
    private long lookupNumOutputVecBatches;
    private long lookupAddInputTime;
    private long lookupGetOutputTime;

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
     * @param cpuNanos time of operator
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
     * @param getOutputTime getOutputTime
     * @param buildInputRows buildInputRows
     * @param buildAddInputTime buildAddInputTime
     * @param buildGetOutputTime buildGetOutputTime
     * @param lookupInputRows lookupInputRows
     * @param buildNumInputVecBatches buildNumInputVecBatches
     * @param lookupNumInputVecBatches lookupNumInputVecBatches
     * @param lookupNumOutputVecBatches lookupNumOutputVecBatches
     * @param addInputTime addInputTime
     * @param lookupAddInputTime lookupAddInputTime
     * @param lookupGetOutputTime lookupGetOutputTime
     * @param lookupOutputRows lookupOutputRows
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
        long cpuNanos,
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
        long numWrittenFiles,

        long addInputTime,
        long getOutputTime,

        long buildInputRows,
        long buildNumInputVecBatches,
        long buildAddInputTime,
        long buildGetOutputTime,

        long lookupInputRows,
        long lookupNumInputVecBatches,
        long lookupOutputRows,
        long lookupNumOutputVecBatches,
        long lookupAddInputTime,
        long lookupGetOutputTime) {
        this.inputRows = inputRows;
        this.numInputVecBatches = inputVectors;
        this.inputBytes = inputBytes;
        this.rawInputRows = rawInputRows;
        this.rawInputBytes = rawInputBytes;
        this.outputRows = outputRows;
        this.numOutputVecBatches = outputVectors;
        this.outputBytes = outputBytes;
        this.cpuCount = cpuCount;
        this.cpuNanos = cpuNanos;
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
        this.scanTime = scanTime;
        this.addInputTime = addInputTime;
        this.getOutputTime = getOutputTime;

        this.buildInputRows = buildInputRows;
        this.buildAddInputTime = buildAddInputTime;
        this.buildGetOutputTime = buildGetOutputTime;
        this.buildNumInputVecBatches = buildNumInputVecBatches;

        this.lookupAddInputTime = lookupAddInputTime;
        this.lookupInputRows = lookupInputRows;
        this.lookupGetOutputTime = lookupGetOutputTime;
        this.lookupOutputRows = lookupOutputRows;
        this.lookupNumInputVecBatches = lookupNumInputVecBatches;
        this.lookupNumOutputVecBatches = lookupNumOutputVecBatches;
    }

    public long getInputRows() {
        return inputRows;
    }

    public void setInputRows(long inputRows) {
        this.inputRows = inputRows;
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

    public long getNumInputVecBatches() {
        return numInputVecBatches;
    }

    public void setNumInputVecBatches(long numInputVecBatches) {
        this.numInputVecBatches = numInputVecBatches;
    }

    public long getNumOutputVecBatches() {
        return numOutputVecBatches;
    }

    public void setNumOutputVecBatches(long numOutputVecBatches) {
        this.numOutputVecBatches = numOutputVecBatches;
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

    public long getCpuNanos() {
        return cpuNanos;
    }

    public void setCpuNanos(long wallNanos) {
        this.cpuNanos = wallNanos;
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

    public long getLookupNumOutputVecBatches() {
        return lookupNumOutputVecBatches;
    }

    public void setLookupNumOutputVecBatches(long lookupNumOutputVecBatches) {
        this.lookupNumOutputVecBatches = lookupNumOutputVecBatches;
    }

    public long getAddInputTime() {
        return addInputTime;
    }

    public void setAddInputTime(long addInputTime) {
        this.addInputTime = addInputTime;
    }

    public long getGetOutputTime() {
        return getOutputTime;
    }

    public void setGetOutputTime(long getOutputTime) {
        this.getOutputTime = getOutputTime;
    }

    public long getBuildInputRows() {
        return buildInputRows;
    }

    public void setBuildInputRows(long buildInputRows) {
        this.buildInputRows = buildInputRows;
    }

    public long getBuildNumInputVecBatches() {
        return buildNumInputVecBatches;
    }

    public void setBuildNumInputVecBatches(long buildNumInputVecBatches) {
        this.buildNumInputVecBatches = buildNumInputVecBatches;
    }

    public long getBuildAddInputTime() {
        return buildAddInputTime;
    }

    public void setBuildAddInputTime(long buildAddInputTime) {
        this.buildAddInputTime = buildAddInputTime;
    }

    public long getBuildGetOutputTime() {
        return buildGetOutputTime;
    }

    public void setBuildGetOutputTime(long buildGetOutputTime) {
        this.buildGetOutputTime = buildGetOutputTime;
    }

    public long getLookupInputRows() {
        return lookupInputRows;
    }

    public void setLookupInputRows(long lookupInputRows) {
        this.lookupInputRows = lookupInputRows;
    }

    public long getLookupNumInputVecBatches() {
        return lookupNumInputVecBatches;
    }

    public void setLookupNumInputVecBatches(long lookupNumInputVecBatches) {
        this.lookupNumInputVecBatches = lookupNumInputVecBatches;
    }

    public long getLookupOutputRows() {
        return lookupOutputRows;
    }

    public void setLookupOutputRows(long lookupOutputRows) {
        this.lookupOutputRows = lookupOutputRows;
    }

    public long getLookupAddInputTime() {
        return lookupAddInputTime;
    }

    public void setLookupAddInputTime(long lookupAddInputTime) {
        this.lookupAddInputTime = lookupAddInputTime;
    }

    public long getLookupGetOutputTime() {
        return lookupGetOutputTime;
    }

    public void setLookupGetOutputTime(long lookupGetOutputTime) {
        this.lookupGetOutputTime = lookupGetOutputTime;
    }
}