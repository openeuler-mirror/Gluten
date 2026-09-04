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

package org.apache.gluten.substrait.expression;

import io.substrait.proto.Expression;

import org.apache.gluten.substrait.type.OmniStringViewTypeNode;

/**
 * A string literal emitted as an Omni StringView literal: the substrait `Expression.Literal` carries
 * `type_variation_reference = 21` so native decodes it as StringView (matching a StringView column in
 * a comparison). Plain {@link StringLiteralNode} drops the type node's variation, so this node sets it
 * explicitly. Extends the serializable {@link LiteralNodeWithValue} (unlike an anonymous ExpressionNode,
 * which is not serializable and breaks Spark task closure serialization).
 *
 * @since 2026
 */
public class OmniStringViewLiteralNode extends LiteralNodeWithValue<String> {
    /**
     * Creates a StringView literal node.
     *
     * @param value literal value
     */
    public OmniStringViewLiteralNode(String value) {
        super(value, new OmniStringViewTypeNode(true));
    }

    @Override
    protected void updateLiteralBuilder(Expression.Literal.Builder literalBuilder, String value) {
        literalBuilder
                .setString(value)
                .setTypeVariationReference(OmniStringViewTypeNode.OMNI_STRING_VIEW_TYPE_VARIATION_REFERENCE);
    }
}
