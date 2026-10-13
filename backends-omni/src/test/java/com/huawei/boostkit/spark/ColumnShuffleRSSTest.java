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

import com.huawei.boostkit.spark.jni.SparkJniWrapper;
import nova.hetu.omniruntime.type.DataType;
import nova.hetu.omniruntime.type.DataTypeSerializer;
import nova.hetu.omniruntime.vector.VecBatch;
import org.junit.Test;
import static org.junit.Assert.*;

public class ColumnShuffleRSSTest extends ColumnShuffleTest {
    public static final class Pusher {
        int pushes;
        long bytes;
        public int pushPartitionData(int partition, byte[] data, int length) {
            assertEquals(0, partition);
            assertTrue(length > 0 && length <= data.length);
            pushes++;
            bytes += length;
            return length;
        }
    }

    @Test
    public void rssUsesTaskManagerAndReturnsReservations() {
        DataType.DataTypeId[] ids = {DataType.DataTypeId.OMNI_INT};
        String schema = DataTypeSerializer.serialize(dataTypeId2DataType(ids));
        SparkJniWrapper wrapper = new SparkJniWrapper();
        Pusher pusher = new Pusher();
        long splitter = wrapper.nativeMakeForRSS("single", 1, schema, 1,
                1024, "lz4", 4096, 1024, Long.MAX_VALUE, Long.MAX_VALUE, pusher, nmmHandle);
        assertTrue(splitter != 0);
        try {
            VecBatch batch = buildVecBatch(ids, 1024, 1, false, false);
            wrapper.split(splitter, batch.getNativeVectorBatch());
            assertTrue("RSS allocations must reach the task reservation listener",
                    reservedNativeBytes() > 0);
            wrapper.stop(splitter);
            assertTrue(pusher.pushes > 0);
            assertTrue(pusher.bytes > 0);
        } finally {
            wrapper.close(splitter);
        }
        // Inherited @After releases NMM and asserts all reservations returned.
    }
}
