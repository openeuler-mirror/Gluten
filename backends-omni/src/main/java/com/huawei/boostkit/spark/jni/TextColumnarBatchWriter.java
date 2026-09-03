/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package com.huawei.boostkit.spark.jni;

import com.huawei.boostkit.write.jni.TextColumnarBatchJniWriter;

import nova.hetu.omniruntime.vector.Vec;

import org.apache.gluten.vectorized.OmniColumnVector;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.json.JSONObject;

import java.net.URI;

/**
 * Writes Spark columnar batches with the native Text writer.
 *
 * @since 2026/08/28
 */
public class TextColumnarBatchWriter {
    private final TextColumnarBatchJniWriter jniWriter = new TextColumnarBatchJniWriter();
    private long writer;

    /**
     * Initializes a native Text writer for the specified output path.
     *
     * @param path output file path
     */
    public void initializeWriterJava(Path path) {
        URI uri = path.toUri();
        JSONObject options = new JSONObject();
        options.put("uri", path.toString());
        options.put("host", uri.getHost() == null ? "" : uri.getHost());
        options.put("scheme", uri.getScheme() == null ? "" : uri.getScheme());
        options.put("port", uri.getPort());
        options.put("path", uri.getPath() == null ? "" : uri.getPath());
        writer = jniWriter.initializeWriter(options);
    }

    /**
     * Writes all rows from the data column of a columnar batch.
     *
     * @param dataColumnIds flags identifying the data column in the batch
     * @param batch columnar batch to write
     * @throws IllegalArgumentException if the column mask is invalid or the data column is not an
     *         {@link OmniColumnVector}
     */
    public void write(boolean[] dataColumnIds, ColumnarBatch batch) {
        write(dataColumnIds, batch, 0, batch.numRows());
    }

    /**
     * Writes a row range from the data column of a columnar batch.
     *
     * @param dataColumnIds flags identifying the data column in the batch
     * @param batch columnar batch to write
     * @param startPos inclusive start row index
     * @param endPos exclusive end row index
     * @throws IllegalArgumentException if the column mask is invalid or the data column is not an
     *         {@link OmniColumnVector}
     */
    public void write(
            boolean[] dataColumnIds, ColumnarBatch batch, long startPos, long endPos) {
        int dataColumnIndex = findDataColumn(dataColumnIds, batch.numCols());
        ColumnVector columnVector = batch.column(dataColumnIndex);
        if (!(columnVector instanceof OmniColumnVector)) {
            throw new IllegalArgumentException("Text writer requires OmniColumnVector input.");
        }
        OmniColumnVector omniVector = (OmniColumnVector) columnVector;
        Vec vector = omniVector.getVec();
        jniWriter.write(writer, vector.getNativeVector(), startPos, endPos);
    }

    private int findDataColumn(boolean[] dataColumnIds, int columnCount) {
        if (dataColumnIds.length != columnCount) {
            throw new IllegalArgumentException("Text writer column mask does not match the batch.");
        }
        int dataColumnIndex = -1;
        for (int index = 0; index < dataColumnIds.length; ++index) {
            if (dataColumnIds[index]) {
                if (dataColumnIndex >= 0) {
                    throw new IllegalArgumentException(
                            "Text writer requires exactly one data column.");
                }
                dataColumnIndex = index;
            }
        }
        if (dataColumnIndex < 0) {
            throw new IllegalArgumentException("Text writer requires exactly one data column.");
        }
        return dataColumnIndex;
    }

    /**
     * Closes the native Text writer if it has been initialized.
     */
    public void close() {
        if (writer != 0) {
            long writerToClose = writer;
            writer = 0;
            jniWriter.close(writerToClose);
        }
    }
}
