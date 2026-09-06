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
package org.apache.gluten.datasources.text

import org.apache.gluten.execution.{
  ProjectExecTransformer,
  RowToColumnarExecBase,
  SortExecTransformer,
  WholeStageTransformer
}
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.ColumnarCollapseTransformStages
import org.apache.spark.sql.execution.ColumnarCollapseTransformStages.transformStageCounter
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.{FakeRowAdaptor, GlutenFormatWriterInjectsBase, OutputWriter}
import org.apache.spark.sql.types.{StringType, StructType}

import java.{util => ju}

class OmniTextFormatWriterInjects extends GlutenFormatWriterInjectsBase {
  override def nativeConf(
      options: Map[String, String],
      compressionCodec: String): ju.Map[String, String] = {
    val normalized =
      if (options.contains(OmniTextOptionsAdapter.SourceKindKey)) {
        options
      } else {
        OmniTextOptionsAdapter
          .fromSparkText(
            options,
            new StructType().add("value", StringType),
            new StructType().add("value", StringType))
          .toProperties
      }
    val result = new ju.HashMap[String, String]()
    normalized.foreach { case (key, value) => result.put(key, value) }
    result
  }

  override def formatName: String = "text"

  override def createOutputWriter(
      outputPath: String,
      dataSchema: StructType,
      context: TaskAttemptContext,
      nativeConf: ju.Map[String, String]): OutputWriter =
    new OmniTextOutputWriter(outputPath, dataSchema, context, nativeConf)

  override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] =
    Some(new StructType().add("value", StringType))

  def execWriterWrappedSparkPlan(plan: SparkPlan): SparkPlan = {
    if (plan.isInstanceOf[FakeRowAdaptor]) {
      return plan
    }
    val transformed = plan match {
      case _: RowToColumnarExecBase => plan
      case _ => transform(plan)
    }

    def injectAdapter(current: SparkPlan): SparkPlan = current match {
      case project: ProjectExecTransformer => project.mapChildren(injectAdapter)
      case sort: SortExecTransformer => sort.mapChildren(injectAdapter)
      case _ => ColumnarCollapseTransformStages.wrapInputIteratorTransformer(current)
    }

    val transformedWithAdapter = injectAdapter(transformed)
    val wholeStage = WholeStageTransformer(transformedWithAdapter, materializeInput = true)(
      transformStageCounter.incrementAndGet())
    FakeRowAdaptor(wholeStage)
  }
}
