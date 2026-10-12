/*
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
package org.apache.spark.sql.hive

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.expression.UDFResolver

/**
 * Detects whether an expression is a Spark Hive UDF (Simple/Generic/UDAF/UDTF).
 */
object OmniHiveUdfUtil {
  def isHiveUdf(expr: Expression): Boolean = expr match {
    case _: HiveSimpleUDF => true
    case _: HiveGenericUDF => true
    case _: HiveUDAFFunction => true
    case _: HiveGenericUDTF => true
    case _ => false
  }

  def hiveUdfClassName(expr: Expression): Option[String] = expr match {
    case s: HiveSimpleUDF => Some(s.funcWrapper.functionClassName)
    case g: HiveGenericUDF => Some(g.funcWrapper.functionClassName)
    case _ => None
  }

  /** True when a matching cpp so has registered this Hive class FQCN into [[UDFResolver.UDFNames]]. */
  def isRegisteredNativeHiveUdf(expr: Expression): Boolean =
    hiveUdfClassName(expr).exists(UDFResolver.UDFNames.contains)
}
