/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */

package org.apache.gluten.connector.write;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/** JSON representation of one Omni-written Paimon data file.
 *
 * @since 2026
 */
public class PaimonFileInfoJson {
    @JsonProperty("path")
    private String path;

    @JsonProperty("partitionValues")
    private List<String> partitionValues;

    @JsonProperty("bucket")
    private int bucket;

    @JsonProperty("recordCount")
    private long recordCount;

    @JsonProperty("fileSizeInBytes")
    private long fileSizeInBytes;

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public List<String> getPartitionValues() {
        return partitionValues;
    }

    public void setPartitionValues(List<String> partitionValues) {
        this.partitionValues = partitionValues;
    }

    public int getBucket() {
        return bucket;
    }

    public void setBucket(int bucket) {
        this.bucket = bucket;
    }

    public long getRecordCount() {
        return recordCount;
    }

    public void setRecordCount(long recordCount) {
        this.recordCount = recordCount;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public void setFileSizeInBytes(long fileSizeInBytes) {
        this.fileSizeInBytes = fileSizeInBytes;
    }
}
