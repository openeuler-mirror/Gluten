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

import java.io.File

import scala.io.Source

/**
 * Drift guard: every string function the Scala [[StringViewCapability]] whitelist claims is
 * StringView-capable MUST have a matching OMNI_STRING_VIEW registration in the native
 * RegisterString.cpp. Without this, a native change that drops an SV overload would silently make
 * the Scala rule keep a column StringView that native can no longer evaluate — the exact class of
 * failure that bit EqualTo ("Vector function not found for binary expression").
 *
 * NOTE on granularity: this checks PRESENCE of an OMNI_STRING_VIEW registration per function name,
 * not the exact overload. The {SV,VARCHAR} vs {SV,SV}-only distinction (why EqualTo is excluded —
 * native has only {SV,SV} comparisons while string literals are VARCHAR) is documented in
 * StringViewCapability and cannot be captured by a name-presence regex; keep that reasoning there.
 *
 * The native source lives in the OmniOperator repo, mounted at /workspace/OmniOperator in the CI
 * container. If it cannot be located (e.g. a Gluten-only checkout), the test is cancelled, not
 * failed, so a partial checkout does not produce a false negative.
 */
class StringViewCapabilityCrossCheckSuite extends SparkFunSuite {

  // Scala SV-whitelist string function -> its native registration name in RegisterString.cpp.
  private val scalaToNativeName: Map[String, String] = Map(
    "StartsWith" -> "StartsWith",
    "EndsWith" -> "EndsWith",
    "Contains" -> "Contains",
    "Like" -> "LIKE",
    "StringInstr" -> "instr",
    "StringLocate" -> "locate",
    "Length" -> "length",
    "Ascii" -> "ascii",
    "Concat" -> "concat",
    "Substring" -> "substr",
    "StringTrim" -> "Trim",
    "StringTrimLeft" -> "LTrim",
    "StringTrimRight" -> "RTrim")

  private val registerStringCandidates: Seq[String] = Seq(
    "/workspace/OmniOperator/core/src/vectorization/registration/RegisterString.cpp",
    "../omni/core/src/vectorization/registration/RegisterString.cpp",
    "../../omni/core/src/vectorization/registration/RegisterString.cpp")

  private def locateRegisterString: Option[File] =
    (sys.env.get("OMNI_SOURCE_ROOT").toSeq
      .map(_ + "/core/src/vectorization/registration/RegisterString.cpp") ++ registerStringCandidates)
      .map(new File(_))
      .find(_.isFile)

  test("Scala StringView expression whitelist stays in sync with native RegisterString.cpp") {
    val file = locateRegisterString.getOrElse(
      cancel("RegisterString.cpp not found (OmniOperator source not mounted); skipping drift check"))

    val src = Source.fromFile(file, "UTF-8")
    val text =
      try src.getLines().mkString("\n")
      finally src.close()

    // Lines that register a StringView overload for some function.
    val svLines = text.linesIterator.filter(_.contains("OMNI_STRING_VIEW")).toSeq

    // A whitelisted function is "missing" if no OMNI_STRING_VIEW registration line mentions its
    // native name.
    val missing = scalaToNativeName.filter {
      case (_, nativeName) => !svLines.exists(_.contains(nativeName))
    }

    assert(
      missing.isEmpty,
      s"Scala StringViewCapability whitelists ${missing.keys.mkString(", ")}, but no OMNI_STRING_VIEW " +
        s"registration was found in ${file.getPath} for native name(s) ${missing.values.mkString(", ")}. " +
        s"Either native dropped SV support (remove from the Scala whitelist) or the native name mapping " +
        s"in this test is stale.")
  }
}
