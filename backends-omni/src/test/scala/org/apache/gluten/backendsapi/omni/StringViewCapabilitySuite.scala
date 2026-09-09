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

package org.apache.gluten.backendsapi.omni

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{
  Ascii,
  Ascending,
  AttributeReference,
  Concat,
  Contains,
  EndsWith,
  EqualTo,
  IsNotNull,
  IsNull,
  Length,
  Literal,
  Lower,
  SortOrder,
  StartsWith,
  StringInstr,
  StringTrim,
  Substring,
  Upper
}
import org.apache.spark.sql.execution.{FilterExec, LocalTableScanExec, ProjectExec, SortExec}
import org.apache.spark.sql.execution.window.WindowExec
import org.apache.spark.sql.types.{IntegerType, StringType}

class StringViewCapabilitySuite extends SparkFunSuite {

  private val s = AttributeReference("s", StringType)()
  private val id = AttributeReference("id", IntegerType)()
  private val scan = LocalTableScanExec(Seq(s, id), Nil)

  test("only Filter/Project are StringView-capable operators") {
    assert(StringViewCapability.operatorSupportsStringView(FilterExec(IsNotNull(s), scan)))
    assert(StringViewCapability.operatorSupportsStringView(ProjectExec(Seq(s), scan)))

    assert(!StringViewCapability.operatorSupportsStringView(
      SortExec(Seq(SortOrder(s, Ascending)), global = true, scan)))
    assert(!StringViewCapability.operatorSupportsStringView(
      WindowExec(Nil, Seq(s), Seq(SortOrder(id, Ascending)), scan)))
    assert(!StringViewCapability.operatorSupportsStringView(scan))
  }

  test("whitelisted expressions support StringView operands") {
    val lit = Literal("x")
    Seq(
      s,
      lit,
      IsNull(s),
      IsNotNull(s),
      EqualTo(s, lit),
      Length(s),
      Ascii(s),
      StartsWith(s, lit),
      EndsWith(s, lit),
      Contains(s, lit),
      StringInstr(s, lit)
    ).foreach(e => assert(StringViewCapability.expressionSupportsStringView(e), s"expected SV-capable: $e"))
  }

  test("Concat supports StringView operands and produces StringView output") {
    val lit = Literal("x")
    assert(StringViewCapability.expressionSupportsStringView(Concat(Seq(s, lit))))
    assert(StringViewCapability.expressionProducesStringView(Concat(Seq(s, lit))))
  }

  test("Substring and trim support StringView operands and produce StringView output") {
    val lit = Literal("x")
    assert(StringViewCapability.expressionSupportsStringView(Substring(s, Literal(1), Literal(3))))
    assert(StringViewCapability.expressionProducesStringView(Substring(s, Literal(1), Literal(3))))
    assert(StringViewCapability.expressionSupportsStringView(StringTrim(s)))
    assert(StringViewCapability.expressionProducesStringView(StringTrim(s)))
    assert(StringViewCapability.expressionSupportsStringView(StringTrim(s, Some(lit))))
    assert(!StringViewCapability.expressionProducesStringView(StringTrim(s, Some(lit))))
  }

  test("non-whitelisted string expressions force VARCHAR") {
    Seq(
      Lower(s),
      Upper(s)
    ).foreach(e => assert(!StringViewCapability.expressionSupportsStringView(e), s"expected not SV-capable: $e"))
  }
}
