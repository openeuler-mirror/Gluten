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
    private long[] numInputVecBatches;

    /** number of input bytes */
    private long[] inputBytes;

    /** number of input times */
    private long[] addInputTime;

    /** number of raw input rows */
    private long[] rawInputRows;

    /** number of raw input bytes */
    private long[] rawInputBytes;

    /** number of output rows */
    private long[] outputRows;

    /** number of output vectors */
    private long[] numOutputVecBatches;

    /** number of output bytes */
    private long[] outputBytes;

    /** number of output times */
    private long[] getOutputTime;

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

    // For BHJ/SHJ
    /** number of build input rows */
    private long[] buildInputRows;

    /** number of build input vector batch */
    private long[] buildNumInputVecBatches;

    /** number of build add input time */
    private long[] buildAddInputTime;

    /** number of lookup output time */
    private long[] buildGetOutputTime;

    /** number of lookup input rows */
    private long[] lookupInputRows;

    /** number of lookup input vector batch */
    private long[] lookupNumInputVecBatches;

    /** number of lookup output row */
    private long[] lookupOutputRows;

    /** number of lookup output vector batch */
    private long[] lookupNumOutputVecBatches;

    /** number of lookup add input time */
    private long[] lookupAddInputTime;

    /** number of lookup output time */
    private long[] lookupGetOutputTime;

    /** single metric */
    private SingleMetric singleMetric = new SingleMetric();

    /**
     * Create an instance for native metrics.
     *
     * @param inputRows number of input rows
     * @param numInputVecBatches number of input vectors
     * @param inputBytes number of input bytes
     * @param rawInputRows number of raw input rows
     * @param rawInputBytes number of raw input bytes
     * @param outputRows number of output rows
     * @param numOutputVecBatches number of output vectors
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
        long[] numInputVecBatches,
        long[] inputBytes,
        long[] rawInputRows,
        long[] rawInputBytes,
        long[] outputRows,
        long[] numOutputVecBatches,
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
        long[] numWrittenFiles,
        long[] addInputTime,
        long[] getOutputTime,
        long[] buildInputRows,
        long[] buildNumInputVecBatches,
        long[] buildAddInputTime,
        long[] buildGetOutputTime,
        long[] lookupInputRows,
        long[] lookupNumInputVecBatches,
        long[] lookupOutputRows,
        long[] lookupNumOutputVecBatches,
        long[] lookupAddInputTime,
        long[] lookupGetOutputTime
        ) {
        this.inputRows = inputRows;
        this.numInputVecBatches = numInputVecBatches;
        this.inputBytes = inputBytes;
        this.rawInputRows = rawInputRows;
        this.rawInputBytes = rawInputBytes;
        this.outputRows = outputRows;
        this.numOutputVecBatches = numOutputVecBatches;
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

    public long[] getInputRows() {
        return inputRows;
    }

    public long[] getInputBytes() {
        return inputBytes;
    }

    public long[] getRawInputRows() {
        return rawInputRows;
    }

    public long[] getRawInputBytes() {
        return rawInputBytes;
    }

    public long[] getOutputRows() {
        return outputRows;
    }

    public long[] getOutputBytes() {
        return outputBytes;
    }

    public long[] getCpuCount() {
        return cpuCount;
    }

    public long[] getWallNanos() {
        return wallNanos;
    }

    public long[] getScanTime() {
        return scanTime;
    }

    public long[] getPeakMemoryBytes() {
        return peakMemoryBytes;
    }

    public long[] getNumMemoryAllocations() {
        return numMemoryAllocations;
    }

    public long[] getSpilledInputBytes() {
        return spilledInputBytes;
    }

    public long[] getSpilledBytes() {
        return spilledBytes;
    }

    public long[] getSpilledRows() {
        return spilledRows;
    }

    public long[] getSpilledPartitions() {
        return spilledPartitions;
    }

    public long[] getSpilledFiles() {
        return spilledFiles;
    }

    public long[] getNumDynamicFiltersProduced() {
        return numDynamicFiltersProduced;
    }

    public long[] getNumDynamicFiltersAccepted() {
        return numDynamicFiltersAccepted;
    }

    public long[] getNumReplacedWithDynamicFilterRows() {
        return numReplacedWithDynamicFilterRows;
    }

    public long[] getFlushRowCount() {
        return flushRowCount;
    }

    public long[] getLoadedToValueHook() {
        return loadedToValueHook;
    }

    public long[] getSkippedSplits() {
        return skippedSplits;
    }

    public long[] getProcessedSplits() {
        return processedSplits;
    }

    public long[] getSkippedStrides() {
        return skippedStrides;
    }

    public long[] getProcessedStrides() {
        return processedStrides;
    }

    public long[] getRemainingFilterTime() {
        return remainingFilterTime;
    }

    public long[] getIoWaitTime() {
        return ioWaitTime;
    }

    public long[] getStorageReadBytes() {
        return storageReadBytes;
    }

    public long[] getLocalReadBytes() {
        return localReadBytes;
    }

    public long[] getRamReadBytes() {
        return ramReadBytes;
    }

    public long[] getPreloadSplits() {
        return preloadSplits;
    }

    public long[] getPhysicalWrittenBytes() {
        return physicalWrittenBytes;
    }

    public long[] getWriteIOTime() {
        return writeIOTime;
    }

    public long[] getNumWrittenFiles() {
        return numWrittenFiles;
    }

    public SingleMetric getSingleMetric() {
        return singleMetric;
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
            numInputVecBatches[index],
            inputBytes[index],
            rawInputRows[index],
            rawInputBytes[index],
            outputRows[index],
            numOutputVecBatches[index],
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
            numWrittenFiles[index],

            addInputTime[index],
            getOutputTime[index],
            buildInputRows[index],
            buildNumInputVecBatches[index],
            buildAddInputTime[index],
            buildGetOutputTime[index],

            lookupNumOutputVecBatches[index],
            lookupNumInputVecBatches[index],
            lookupOutputRows[index],
            lookupInputRows[index],
            lookupGetOutputTime[index],
            lookupAddInputTime[index]
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