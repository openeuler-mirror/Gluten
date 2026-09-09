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

package org.apache.gluten.substrait.type;

import io.substrait.proto.Type;

import java.io.Serializable;

/**
 * Self-describing wire type node for Omni StringView. A Substrait {@code string} carrying this
 * type_variation_reference is decoded by the native parser as OMNI_STRING_VIEW, independent of any
 * session config. Plain {@code string} (variation 0, {@link StringTypeNode}) means standard VARCHAR.
 * The native contract lives in {@code SubstraitParser.h}
 * (OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE) and MUST match {@link
 * #OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE} here.
 *
 * @since 2026
 */
public class OmniStringViewTypeNode implements TypeNode, Serializable {
    /** StringView type variation reference shared with the native Substrait parser. */
    public static final int OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE = 21;

    private static final long serialVersionUID = 1L;

    private final Boolean isNullable;

    /**
     * Creates a StringView type node.
     *
     * @param isNullable whether the type accepts null values
     */
    public OmniStringViewTypeNode(Boolean isNullable) {
        this.isNullable = isNullable;
    }

    @Override
    public Type toProtobuf() {
        Type.String.Builder stringBuilder = Type.String.newBuilder();
        stringBuilder.setTypeVariationReference(OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE);
        if (isNullable) {
            stringBuilder.setNullability(Type.Nullability.NULLABILITY_NULLABLE);
        } else {
            stringBuilder.setNullability(Type.Nullability.NULLABILITY_REQUIRED);
        }

        Type.Builder builder = Type.newBuilder();
        builder.setString(stringBuilder.build());
        return builder.build();
    }
}
