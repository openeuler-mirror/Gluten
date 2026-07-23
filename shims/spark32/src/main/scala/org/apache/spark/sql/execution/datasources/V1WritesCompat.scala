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
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeSet, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.command.DataWritingCommand
import org.apache.spark.sql.execution.datasources.FileFormatWriter.Empty2Null
import org.apache.spark.sql.types.StringType

case class WriterBucketSpec(bucketIdExpression: Expression, fileNamePrefix: Int => String)

trait V1WriteCommand extends DataWritingCommand {
  def fileFormat: FileFormat
  def partitionColumns: Seq[Attribute]
  def staticPartitions: TablePartitionSpec
  def bucketSpec: Option[BucketSpec]
  def options: Map[String, String]
  def requiredOrdering: Seq[SortOrder]
}

object V1WritesUtils {
  def getWriterBucketSpec(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute],
      options: Map[String, String]): Option[WriterBucketSpec] = {
    bucketSpec.map { spec =>
      val bucketColumns = spec.bucketColumnNames.map(c => dataColumns.find(_.name == c).get)
      val bucketIdExpression =
        org.apache.spark.sql.catalyst.plans.physical.HashPartitioning(
          bucketColumns,
          spec.numBuckets).partitionIdExpression
      WriterBucketSpec(bucketIdExpression, (_: Int) => "")
    }
  }

  def getBucketSortColumns(
      bucketSpec: Option[BucketSpec],
      dataColumns: Seq[Attribute]): Seq[Attribute] = {
    bucketSpec.toSeq.flatMap {
      spec => spec.sortColumnNames.map(c => dataColumns.find(_.name == c).get)
    }
  }

  def getSortOrder(
      outputColumns: Seq[Attribute],
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      numStaticPartitionCols: Int = 0): Seq[SortOrder] = {
    require(partitionColumns.size >= numStaticPartitionCols)
    val partitionSet = AttributeSet(partitionColumns)
    val dataColumns = outputColumns.filterNot(partitionSet.contains)
    val bucketIdExpression = getWriterBucketSpec(bucketSpec, dataColumns, options).map(_.bucketIdExpression)
    val sortColumns = getBucketSortColumns(bucketSpec, dataColumns)
    (partitionColumns.drop(numStaticPartitionCols) ++ bucketIdExpression ++ sortColumns)
      .map(SortOrder(_, org.apache.spark.sql.catalyst.expressions.Ascending))
  }

  def convertEmptyToNull(
      output: Seq[Attribute],
      partitionColumns: Seq[Attribute]): Seq[NamedExpression] = {
    val partitionSet = AttributeSet(partitionColumns)
    var needConvert = false
    val projectList: Seq[NamedExpression] = output.map {
      case p if partitionSet.contains(p) && p.dataType == StringType && p.nullable =>
        needConvert = true
        Alias(Empty2Null(p), p.name)()
      case attr => attr
    }
    if (needConvert) projectList else Nil
  }

  def isOrderingMatched(
      requiredOrdering: Seq[Expression],
      outputOrdering: Seq[SortOrder]): Boolean = {
    if (requiredOrdering.length > outputOrdering.length) {
      false
    } else {
      requiredOrdering.zip(outputOrdering).forall {
        case (requiredOrder, outputOrder) =>
          outputOrder.satisfies(outputOrder.copy(child = requiredOrder))
      }
    }
  }

  def getWriteFilesOpt(child: SparkPlan): Option[WriteFilesExec] = {
    child.collectFirst {
      case w: WriteFilesExec => w
    }
  }
}
