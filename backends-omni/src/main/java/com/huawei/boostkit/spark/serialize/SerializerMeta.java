/*
 * Copyright (C) 2026-2026. Huawei Technologies Co., Ltd. All rights reserved.
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

package com.huawei.boostkit.spark.serialize;

/**
 * Serializer meta info to build ColumnVector
 *
 * @since 2026/03/04
 */
public class SerializerMeta {
    int rowCount;
    int vecCount;

    int[] typeIdArray;
    int[] precisionArray;
    int[] scaleArray;
    long[] vecNativeIdArray;

    long batchHandle;


    public int getRowCount() {
        return rowCount;
    }

    public int getVecCount() {
        return vecCount;
    }

    public int[] getTypeIdArray() {
        return typeIdArray;
    }

    public int[] getPrecisionArray() {
        return precisionArray;
    }

    public int[] getScaleArray() {
        return scaleArray;
    }

    public long[] getVecNativeIdArray() {
        return vecNativeIdArray;
    }

    public long getBatchHandle() {
        return batchHandle;
    }
}
