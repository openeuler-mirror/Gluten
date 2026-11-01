/*
 * Copyright (C) 2020-2022. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark;

import java.io.File;
import java.util.LinkedHashSet;
import java.util.Set;
import com.huawei.boostkit.spark.jni.SparkJniWrapper;
import org.apache.gluten.memory.OmniNativeMemoryManagerJniWrapper;
import org.apache.gluten.memory.listener.ReservationListener;
import org.junit.Before;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import nova.hetu.omniruntime.type.CharDataType;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.Date32DataType;
import nova.hetu.omniruntime.type.Date64DataType;
import nova.hetu.omniruntime.type.Decimal128DataType;
import nova.hetu.omniruntime.type.Decimal64DataType;
import nova.hetu.omniruntime.type.DoubleDataType;
import nova.hetu.omniruntime.type.IntDataType;
import nova.hetu.omniruntime.type.LongDataType;
import nova.hetu.omniruntime.type.TimestampDataType;
import nova.hetu.omniruntime.type.VarcharDataType;
import nova.hetu.omniruntime.vector.Decimal128Vec;
import nova.hetu.omniruntime.vector.DoubleVec;
import nova.hetu.omniruntime.vector.IntVec;
import nova.hetu.omniruntime.vector.LongVec;
import nova.hetu.omniruntime.vector.VarcharVec;
import nova.hetu.omniruntime.vector.Vec;
import nova.hetu.omniruntime.vector.VecBatch;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

abstract class ColumnShuffleTest {
    public static String shuffleTestDir = "/tmp/shuffleTests";

    protected long nmmHandle;
    private final Set<Long> openSplitters = new LinkedHashSet<>();
    private long reservedBytes;
    private final SparkJniWrapper cleanupWrapper = new SparkJniWrapper();

    @Before
    public void createNativeMemoryManager() {
        String nativeLibrary = System.getProperty("omni.test.nativeLibrary");
        if (nativeLibrary == null) {
            System.loadLibrary("spark_columnar_plugin");
        } else {
            System.load(nativeLibrary);
        }
        ReservationListener listener = new ReservationListener() {
            @Override
            public synchronized long reserve(long size) {
                reservedBytes = Math.addExact(reservedBytes, size);
                return size;
            }

            @Override
            public synchronized long unreserve(long size) {
                if (size > reservedBytes) {
                    throw new IllegalStateException("Native test reservation underflow");
                }
                reservedBytes -= size;
                return size;
            }

            @Override
            public synchronized long getUsedBytes() {
                return reservedBytes;
            }
        };
        nmmHandle = OmniNativeMemoryManagerJniWrapper.create(
                "omni", listener, new byte[0], "shuffle-test", "test", "1", -1L);
        assertTrue("Native manager must be valid", nmmHandle > 0);
    }

    protected long reservedNativeBytes() {
        return reservedBytes;
    }

    protected SparkJniWrapper newTestShuffleWrapper() {
        return new SparkJniWrapper() {
            @Override
            public long nativeMake(String name, int partitions, String types, int columns,
                    int bufferSize, String codec, String dataFile, int subDirs, String localDirs,
                    long compressionBlockSize, int spillRows, long taskThreshold,
                    long executorThreshold, long handle, long taskOffHeapBytes) {
                assertEquals(nmmHandle, handle);
                long splitter = super.nativeMake(name, partitions, types, columns, bufferSize,
                        codec, dataFile, subDirs, localDirs, compressionBlockSize, spillRows,
                        taskThreshold, executorThreshold, handle, taskOffHeapBytes);
                openSplitters.add(splitter);
                return splitter;
            }

            @Override
            public void close(long splitter) {
                super.close(splitter);
                openSplitters.remove(splitter);
            }
        };
    }

    @After
    public void releaseNativeMemoryManager() {
        // Even failed assertions must close splitters before destroying their pool.
        for (long splitter : new ArrayList<>(openSplitters)) {
            cleanupWrapper.close(splitter);
            openSplitters.remove(splitter);
        }
        if (nmmHandle > 0) {
            OmniNativeMemoryManagerJniWrapper.release(nmmHandle);
            nmmHandle = 0;
        }
        assertEquals("Native reservations must be fully returned", 0L, reservedBytes);
    }


    public DataType[] dataTypeId2DataType(DataType.DataTypeId[] idTypes) {
        DataType[] types = new DataType[idTypes.length];
        for(int i = 0; i < idTypes.length; i++) {
            switch (idTypes[i]) {
                case OMNI_INT: {
                    types[i] = IntDataType.INTEGER;
                    break;
                }
                case OMNI_LONG: {
                    types[i] = LongDataType.LONG;
                    break;
                }
                case OMNI_TIMESTAMP: {
                    types[i] = TimestampDataType.TIMESTAMP;
                    break;
                }
                case OMNI_DOUBLE: {
                    types[i] = DoubleDataType.DOUBLE;
                    break;
                }
                case OMNI_VARCHAR: {
                    types[i] = VarcharDataType.VARCHAR;
                    break;
                }
                case OMNI_CHAR: {
                    types[i] = CharDataType.CHAR;
                    break;
                }
                case OMNI_DATE32: {
                    types[i] = Date32DataType.DATE32;
                    break;
                }
                case OMNI_DATE64: {
                    types[i] = Date64DataType.DATE64;
                    break;
                }
                case OMNI_DECIMAL64: {
                    types[i] = Decimal64DataType.DECIMAL64;
                    break;
                }
                case OMNI_DECIMAL128: {
                    types[i] = Decimal128DataType.DECIMAL128; // Or types[i] = new Decimal128DataType(2, 0);
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("Unsupported type : " + idTypes[i]);
                }
            }
        }
        return types;
    }

    public VecBatch buildVecBatch(DataType.DataTypeId[] idTypes, int rowNum, int partitionNum, boolean mixHalfNull, boolean withPidVec) {
        List<Vec> columns = new ArrayList<>();
        Vec tmpVec = null;
        // prepare pidVec
        if (withPidVec) {
            IntVec pidVec = new IntVec(rowNum);
            for (int i = 0; i < rowNum; i++) {
                pidVec.set(i, i % partitionNum);
            }
            columns.add(pidVec);
        }

        for(int i = 0; i < idTypes.length; i++) {
            switch (idTypes[i]) {
                case OMNI_INT:
                case OMNI_DATE32:{
                    tmpVec = new IntVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((IntVec)tmpVec).set(j, j + 1);
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_LONG:
                case OMNI_TIMESTAMP:
                case OMNI_DECIMAL64:
                case OMNI_DATE64: {
                    tmpVec = new LongVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((LongVec)tmpVec).set(j, j + 1);
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_DOUBLE: {
                    tmpVec = new DoubleVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((DoubleVec)tmpVec).set(j, j + 1);
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_VARCHAR:
                case OMNI_CHAR: {
                    tmpVec = new VarcharVec(rowNum);
                    for (int j = 0; j < rowNum; j++) {
                        ((VarcharVec)tmpVec).set(j, ("VAR_" + (j + 1) + "_END").getBytes(StandardCharsets.UTF_8));
                        if (mixHalfNull && (j % 2) == 0) {
                            tmpVec.setNull(j);
                        }
                    }
                    break;
                }
                case OMNI_DECIMAL128: {
                    long[][] arr = new long[rowNum][2];
                    for (int j = 0; j < rowNum; j++) {
                        arr[j][0] = 2 * j;
                        arr[j][1] = 2 * j + 1;
                        if (mixHalfNull && (j % 2) == 0) {
                            arr[j] = null;
                        }
                    }
                    tmpVec = createDecimal128Vec(arr);
                    break;
                }
                default: {
                    throw new UnsupportedOperationException("Unsupported type : " + idTypes[i]);
                }
            }
            columns.add(tmpVec);
        }
        return new VecBatch(columns);
    }

    public Decimal128Vec createDecimal128Vec(long[][] data) {
        Decimal128Vec result = new Decimal128Vec(data.length);
        for (int i = 0; i < data.length; i++) {
            if (data[i] == null) {
                result.setNull(i);
            } else {
                result.set(i, new long[]{data[i][0], data[i][1]});
            }
        }
        return result;
    }

    public List<Vec> buildValInt(int pid, int val) {
        IntVec c0 = new IntVec(1);
        IntVec c1 = new IntVec(1);
        c0.set(0, pid);
        c1.set(0, val);
        List<Vec> columns = new ArrayList<>();
        columns.add(c0);
        columns.add(c1);
        return columns;
    }

    public List<Vec> buildValChar(int pid, String varChar) {
        IntVec c0 = new IntVec(1);
        VarcharVec c1 = new VarcharVec(1);
        c0.set(0, pid);
        c1.set(0, varChar.getBytes(StandardCharsets.UTF_8));
        List<Vec> columns = new ArrayList<>();
        columns.add(c0);
        columns.add(c1);
        return columns;
    }

    public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }
        return dir.delete();
    }
}
