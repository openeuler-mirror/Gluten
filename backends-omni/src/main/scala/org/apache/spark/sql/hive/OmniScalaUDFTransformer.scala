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

import org.apache.gluten.exception.GlutenNotSupportException
import org.apache.gluten.expression.{
  ExpressionConverter,
  ExpressionTransformer,
  GenericExpressionTransformer,
  UDFMappings
}
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, ScalaUDF}
import org.apache.spark.sql.expression.UDFResolver

object OmniScalaUDFTransformer {
  def replaceWithExpressionTransformer(
      expr: Expression,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    expr match {
      case udf: ScalaUDF =>
        OmniScalaUdfUtil.registeredNativeName(udf) match {
          case Some(registeredName) =>
            val alias = OmniScalaUdfUtil.scalaUdfSqlName(udf).getOrElse(registeredName)
            val udfExpression = UDFResolver.getUdfExpression(registeredName, alias)(udf.children)
            udfExpression.getTransformer(
              ExpressionConverter.replaceWithExpressionTransformer(
                udfExpression.children,
                attributeSeq)
            )
          case None =>
            mappedOrUnsupported(udf, attributeSeq)
        }
      case _ =>
        throw new GlutenNotSupportException(s"Expression $expr is not a ScalaUDF")
    }
  }

  private def mappedOrUnsupported(
      udf: ScalaUDF,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = {
    if (udf.udfName.isEmpty) {
      throw new GlutenNotSupportException("UDF name is not found!")
    }
    UDFMappings.scalaUDFMap.get(udf.udfName.get) match {
      case Some(name) =>
        GenericExpressionTransformer(
          name,
          ExpressionConverter.replaceWithExpressionTransformer(udf.children, attributeSeq),
          udf)
      case _ =>
        throw new GlutenNotSupportException(s"Not supported scala udf: $udf.")
    }
  }
}
