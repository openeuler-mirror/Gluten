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
package org.apache.spark.sql.execution

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.extension.ValidationResult
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.connector.write.WriterCommitMessage
import org.apache.spark.sql.execution.datasources._

import java.util.Date


case class OmniColumnarWriteFilesExec private (
                                                override val left: SparkPlan,
                                                override val right: SparkPlan,
                                                t: WriteFilesExecTransformer,
                                                fileFormat: FileFormat,
                                                partitionColumns: Seq[Attribute],
                                                bucketSpec: Option[BucketSpec],
                                                options: Map[String, String],
                                                staticPartitions: TablePartitionSpec)
  extends ColumnarWriteFilesExec(left, right) {

  override protected def doValidateInternal(): ValidationResult = {
    BackendsApiManager.getSettings.supportWriteFilesExec(
      fileFormat,
      left.output.toStructType.fields,
      bucketSpec,
      CaseInsensitiveMap(options))
  }

  override def doExecuteWrite(writeFilesSpec: WriteFilesSpec): RDD[WriterCommitMessage] = {
    val rdd = child.execute()
    // SPARK-23271 If we are attempting to write a zero partition rdd, create a dummy single
    // partition rdd to make sure we at least set up one write task to write the metadata.
    val rddWithNonEmptyPartitions = if (rdd.partitions.length == 0) {
      session.sparkContext.parallelize(Array.empty[InternalRow], 1)
    } else {
      rdd
    }

    val concurrentOutputWriterSpec = writeFilesSpec.concurrentOutputWriterSpecFunc(child)
    val description = writeFilesSpec.description
    val committer = writeFilesSpec.committer
    val jobIdInstant = new Date().getTime
    rddWithNonEmptyPartitions.mapPartitionsInternal { iterator =>
      val sparkStageId = TaskContext.get().stageId()
      val sparkPartitionId = TaskContext.get().partitionId()
      val sparkAttemptNumber = TaskContext.get().taskAttemptId().toInt & Int.MaxValue

      val ret = OmniFileFormatWriter.executeTask(
        description,
        jobIdInstant,
        sparkStageId,
        sparkPartitionId,
        sparkAttemptNumber,
        committer,
        iterator,
        concurrentOutputWriterSpec
      )
      Iterator(ret)
    }
  }

  override protected def withNewChildrenInternal(
                                                  newLeft: SparkPlan,
                                                  newRight: SparkPlan): SparkPlan =
    copy(newLeft, newRight, t, fileFormat, partitionColumns, bucketSpec, options, staticPartitions)
}
