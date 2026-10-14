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

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.expression.{ExpressionUtils, OmniProjection}
import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.extension.columnar.transition.Convention
import org.apache.gluten.iterator.Iterators
import org.apache.gluten.sql.shims.SparkShimLoader
import org.apache.gluten.vectorized.{OmniColumnVector, OmniColumnVectorBatchComposer, OmniColumnarRow}

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression}
import org.apache.spark.sql.execution.{ExplainUtils, ProjectExec, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vectorized.{MutableColumnarRow, WritableColumnVector}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Omni partial project: consume the native project result, materialize final UDF/Hive UDF outputs
 * as [[OmniColumnVector]] on the JVM, then compose the final project output in original order.
 */
case class OmniColumnarPartialProjectExec(original: ProjectExec, child: SparkPlan)(
    fallbackProjectList: Seq[NamedExpression],
    outputInputIndexes: Seq[Int],
    outputFallbackIndexes: Seq[Int])
  extends UnaryExecNode
  with ValidatablePlan {

  private val udfInputMeta =
    OmniPartialProjectSplit.buildUdfInputMeta(child, fallbackProjectList, forOmniBackend = true)
  private val projectAttributes: Seq[Attribute] = udfInputMeta.projectAttributes
  private val projectIndexInChild: Seq[Int] = udfInputMeta.projectIndexInChild
  private val UDFAttrNotExists: Boolean = udfInputMeta.udfAttrNotExists
  private val hasUnsupportedDataType: Boolean = udfInputMeta.hasUnsupportedDataType

  @transient override lazy val metrics = Map(
    "time" -> SQLMetrics.createTimingMetric(sparkContext, "total time of Omni partial project"),
    "udfEvalTime" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "UDF eval, row read, and writes into OmniColumnVector"),
    "columnMergeTime" -> SQLMetrics.createTimingMetric(
      sparkContext,
      "horizontal Omni batch compose (deep-copy merge)")
  )

  override def output: Seq[Attribute] = original.output

  override def batchType(): Convention.BatchType = BackendsApiManager.getSettings.primaryBatchType

  override def rowType0(): Convention.RowType = Convention.RowType.None

  final override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException(
      s"${this.getClass.getSimpleName} doesn't support doExecute")
  }

  final override protected def otherCopyArgs: Seq[AnyRef] = {
    fallbackProjectList :: outputInputIndexes :: outputFallbackIndexes :: Nil
  }

  override protected def doValidateInternal(): ValidationResult = {
    if (!GlutenConfig.get.enableColumnarPartialProject) {
      return ValidationResult.failed("Config disable this feature")
    }
    if (UDFAttrNotExists) {
      return ValidationResult.failed("Attribute in the UDF does not exists in its child")
    }
    if (hasUnsupportedDataType) {
      return ValidationResult.failed("Attribute in the UDF contains unsupported type")
    }
    if (projectAttributes.size == original.child.output.size) {
      return ValidationResult.failed("UDF need all the columns in original child output")
    }
    if (original.output.isEmpty) {
      return ValidationResult.failed("Project fallback because output is empty")
    }
    if (fallbackProjectList.isEmpty) {
      return ValidationResult.failed("No UDF")
    }
    if (fallbackProjectList.size > original.output.size) {
      return ValidationResult.failed("Number of RowToColumn columns is more than ProjectExec")
    }
    if (
      outputInputIndexes.size != original.output.size ||
      outputFallbackIndexes.size != original.output.size
    ) {
      return ValidationResult.failed("Partial project output mapping size mismatch")
    }
    if (!original.projectList.forall(OmniPartialProjectSplit.validateExpression)) {
      return ValidationResult.failed("Contains expression not supported")
    }
    if (
      ExpressionUtils.hasComplexExpressions(original, GlutenConfig.get.fallbackExpressionsThreshold)
    ) {
      return ValidationResult.failed("Fallback by complex expression")
    }
    ValidationResult.succeeded
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    val totalTime = longMetric("time")
    val udfEvalTime = longMetric("udfEvalTime")
    val columnMergeTime = longMetric("columnMergeTime")

    child.executeColumnar().mapPartitions {
      batches =>
        val res: Iterator[Iterator[(ColumnarBatch, ColumnarBatch)]] =
          new Iterator[Iterator[(ColumnarBatch, ColumnarBatch)]] {
          override def hasNext: Boolean = batches.hasNext

          override def next(): Iterator[(ColumnarBatch, ColumnarBatch)] = {
            val batch = batches.next()
            if (batch.numRows == 0) {
              Iterator.empty
            } else {
              val startTotal = System.currentTimeMillis()
              val numRows = batch.numRows()
              val inputColumns = new Array[WritableColumnVector](projectIndexInChild.size)
              var c = 0
              while (c < projectIndexInChild.size) {
                val childIndex = projectIndexInChild(c)
                val col = batch.column(childIndex)
                inputColumns(c) = col.asInstanceOf[WritableColumnVector]
                c += 1
              }
              val inputRow = new MutableColumnarRow(inputColumns)

              val proj = OmniProjection.create(fallbackProjectList, projectAttributes)
              val schema =
                SparkShimLoader.getSparkShims.structFromAttributes(
                  fallbackProjectList.map(_.toAttribute))
              val udfVectors = OmniColumnVector.allocateColumns(numRows, schema, true)
              val targetRow = new OmniColumnarRow(udfVectors)
              val udfEval = proj.target(targetRow)

              val udfStart = System.currentTimeMillis()
              var i = 0
              while (i < numRows) {
                inputRow.rowId = i
                targetRow.setRowId(i)
                udfEval.apply(inputRow)
                i += 1
              }
              udfEvalTime += System.currentTimeMillis() - udfStart

              val mergeStart = System.currentTimeMillis()
              val composite = OmniColumnVectorBatchComposer.compose(
                batch,
                udfVectors,
                outputInputIndexes.toArray,
                outputFallbackIndexes.toArray)
              columnMergeTime += System.currentTimeMillis() - mergeStart
              totalTime += System.currentTimeMillis() - startTotal

              // The returned batch owns copied passthrough columns and UDF columns; recycle only the
              // original input batch here.
              Iterator.single((batch, composite))
            }
          }
        }
        Iterators
          .wrap(res.flatten)
          .protectInvocationFlow()
          .recyclePayload {
            case (inputBatch, _) =>
              inputBatch.close()
          }
          .create()
          .map(_._2)
    }
  }

  override def verboseStringWithOperatorId(): String = {
    s"""
       |$formattedNodeName
       |${ExplainUtils.generateFieldString("Output", output)}
       |${ExplainUtils.generateFieldString("Input", child.output)}
       |${ExplainUtils.generateFieldString("UDF", fallbackProjectList)}
       |${ExplainUtils.generateFieldString("ProjectOutput", projectAttributes)}
       |${ExplainUtils.generateFieldString("ProjectInputIndex", projectIndexInChild)}
       |${ExplainUtils.generateFieldString("OutputInputIndex", outputInputIndexes)}
       |${ExplainUtils.generateFieldString("OutputFallbackIndex", outputFallbackIndexes)}
       |""".stripMargin
  }

  override def simpleString(maxFields: Int): String =
    super.simpleString(maxFields) + " OmniPartialProject " + fallbackProjectList

  override protected def withNewChildInternal(newChild: SparkPlan): OmniColumnarPartialProjectExec = {
    copy(child = newChild)(fallbackProjectList, outputInputIndexes, outputFallbackIndexes)
  }
}

object OmniColumnarPartialProjectExec {

  def create(original: ProjectExec): OmniColumnarPartialProjectExec = {
    val split = OmniPartialProjectSplit.splitProjectList(original)
    val nativeProject = ProjectExecTransformer(split.nativeProjectList, original.child)
    OmniColumnarPartialProjectExec(original, nativeProject)(
      split.fallbackProjectList,
      split.outputInputIndexes,
      split.outputFallbackIndexes)
  }
}
