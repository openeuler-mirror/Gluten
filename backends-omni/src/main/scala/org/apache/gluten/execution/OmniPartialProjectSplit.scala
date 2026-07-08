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
package org.apache.gluten.execution

import org.apache.spark.sql.catalyst.expressions.{
  Attribute,
  AttributeReference,
  Expression,
  LambdaFunction,
  NamedExpression,
  ScalaUDF
}
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.hive.OmniHiveUdfUtil
import org.apache.spark.sql.types._

import scala.collection.mutable.ListBuffer

/**
 * Splits a Project into native expressions and fallback UDF expressions for Omni partial project.
 */
object OmniPartialProjectSplit {

  def projectListNeedsPartialSplit(projectList: Seq[Expression]): Boolean =
    projectList.exists(containsUDF)

  def containsUDF(expr: Expression): Boolean = {
    if (expr == null) return false
    expr match {
      case _: ScalaUDF => true
      case h if OmniHiveUdfUtil.isHiveUdf(h) => true
      case p => p.children.exists(c => containsUDF(c))
    }
  }

  def validateExpression(expr: Expression): Boolean = {
    expr.deterministic && !expr.isInstanceOf[LambdaFunction] &&
    expr.children.forall(validateExpression)
  }

  /**
   * @param forOmniBackend
   *   if true, exclude types that [[org.apache.gluten.vectorized.OmniColumnVector]] cannot
   *   reserve (e.g. year-month interval).
   */
  def validateDataType(dataType: DataType, forOmniBackend: Boolean): Boolean = {
    dataType match {
      case _: BooleanType => true
      case _: ByteType => true
      case _: ShortType => true
      case _: IntegerType => true
      case _: LongType => true
      case _: FloatType => true
      case _: DoubleType => true
      case _: StringType => true
      case _: TimestampType => true
      case _: DateType => true
      case _: BinaryType => true
      case _: DecimalType => true
      case YearMonthIntervalType.DEFAULT if !forOmniBackend => true
      case _: NullType => true
      case _ => false
    }
  }

  case class UdfInputMeta(
      projectAttributes: Seq[Attribute],
      projectIndexInChild: Seq[Int],
      udfAttrNotExists: Boolean,
      hasUnsupportedDataType: Boolean)

  case class ProjectSplitResult(
      nativeProjectList: Seq[NamedExpression],
      fallbackProjectList: Seq[NamedExpression],
      outputInputIndexes: Seq[Int],
      outputFallbackIndexes: Seq[Int])

  def buildUdfInputMeta(
      child: SparkPlan,
      fallbackProjectList: Seq[NamedExpression],
      forOmniBackend: Boolean): UdfInputMeta = {
    val projectAttributes = ListBuffer[Attribute]()
    val projectIndexInChild = ListBuffer[Int]()
    var udfAttrNotExists = false
    var hasUnsupportedDataType =
      fallbackProjectList.exists(a => !validateDataType(a.dataType, forOmniBackend))

    def getProjectIndexInChildOutput(exprs: Seq[Expression]): Boolean = {
      exprs.forall {
        case a: AttributeReference =>
          val index = child.output.indexWhere(s => s.exprId.equals(a.exprId))
          if (index < 0) {
            udfAttrNotExists = true
            false
          } else if (!validateDataType(a.dataType, forOmniBackend)) {
            hasUnsupportedDataType = true
            false
          } else if (!projectIndexInChild.contains(index)) {
            projectAttributes.append(a.toAttribute)
            projectIndexInChild.append(index)
            true
          } else true
        case p =>
          getProjectIndexInChildOutput(p.children)
          true
      }
    }

    if (!hasUnsupportedDataType) {
      getProjectIndexInChildOutput(fallbackProjectList)
    }

    UdfInputMeta(
      projectAttributes.toSeq,
      projectIndexInChild.toSeq,
      udfAttrNotExists,
      hasUnsupportedDataType)
  }

  def splitProjectList(original: ProjectExec): ProjectSplitResult = {
    val nativeProjectList = ListBuffer[NamedExpression]()
    val fallbackProjectList = ListBuffer[NamedExpression]()
    val outputInputIndexes = ListBuffer[Int]()
    val outputFallbackIndexes = ListBuffer[Int]()

    def nativeOutputIndex(attr: Attribute): Int =
      nativeProjectList.indexWhere(_.toAttribute.exprId == attr.exprId)

    def appendNative(expr: NamedExpression): Int = {
      nativeProjectList.append(expr)
      nativeProjectList.size - 1
    }

    def ensureNativeInput(attr: Attribute): Unit = {
      if (nativeOutputIndex(attr) < 0) {
        appendNative(attr)
      }
    }

    original.projectList.foreach {
      expr =>
        if (containsUDF(expr)) {
          expr.references.foreach(ensureNativeInput)
          fallbackProjectList.append(expr)
          outputInputIndexes.append(-1)
          outputFallbackIndexes.append(fallbackProjectList.size - 1)
        } else {
          outputInputIndexes.append(appendNative(expr))
          outputFallbackIndexes.append(-1)
        }
    }

    if (nativeProjectList.isEmpty && original.child.output.nonEmpty) {
      appendNative(original.child.output.head)
    }

    ProjectSplitResult(
      nativeProjectList.toSeq,
      fallbackProjectList.toSeq,
      outputInputIndexes.toSeq,
      outputFallbackIndexes.toSeq)
  }
}
