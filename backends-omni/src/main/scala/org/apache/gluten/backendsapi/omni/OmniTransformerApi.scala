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
package org.apache.gluten.backendsapi.omni

import org.apache.gluten.backendsapi.TransformerApi
import org.apache.gluten.execution.WriteFilesExecTransformer
import org.apache.gluten.expression.ConverterUtils
import org.apache.gluten.substrait.expression.{ExpressionBuilder, ExpressionNode}
import org.apache.gluten.utils.InputPartitionsUtil

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, PartitionDirectory}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataType, DecimalType, StructType}
import org.apache.spark.util.collection.BitSet

import com.google.protobuf
import com.google.protobuf.{Any, Message, StringValue}

class OmniTransformerApi extends TransformerApi with Logging {

  /** Generate Seq[InputPartition] for FileSourceScanExecTransformer. */
  override def genInputPartitionSeq(
      relation: HadoopFsRelation,
      requiredSchema: StructType,
      selectedPartitions: Array[PartitionDirectory],
      output: Seq[Attribute],
      bucketedScan: Boolean,
      optionalBucketSet: Option[BitSet],
      optionalNumCoalescedBuckets: Option[Int],
      disableBucketedScan: Boolean,
      filterExprs: Seq[Expression]): Seq[InputPartition] = {
    InputPartitionsUtil(
      relation,
      requiredSchema,
      selectedPartitions,
      output,
      bucketedScan,
      optionalBucketSet,
      optionalNumCoalescedBuckets,
      disableBucketedScan)
      .genInputPartitionSeq()
  }

  override def createCheckOverflowExprNode(
      args: Object,
      substraitExprName: String,
      childNode: ExpressionNode,
      childResultType: DataType,
      dataType: DecimalType,
      nullable: Boolean,
      nullOnOverflow: Boolean): ExpressionNode = {
    if (childResultType.equals(dataType)) {
      childNode
    } else {
      val typeNode = ConverterUtils.getTypeNode(dataType, nullable)
      ExpressionBuilder.makeCast(typeNode, childNode, !nullOnOverflow)
    }
  }

  override def getNativePlanString(substraitPlan: Array[Byte], details: Boolean): String = {
    "Omni Test"
  }

  override def packPBMessage(message: Message): protobuf.Any = Any.pack(message, "")

  override def genWriteParameters(write: WriteFilesExecTransformer): protobuf.Any = {

    val fileFormatStr = write.fileFormat match {
      case register: DataSourceRegister =>
        register.shortName
      case _ => "UnknownFileFormat"
    }
    val compressionCodec =
      WriteFilesExecTransformer.getCompressionCodec(write.caseInsensitiveOptions).capitalize
    val writeParametersStr = new StringBuffer("WriteParameters:")
    writeParametersStr.append("is").append(compressionCodec).append("=1")
    writeParametersStr.append(";format=").append(fileFormatStr).append("\n")

    packPBMessage(
      StringValue
        .newBuilder()
        .setValue(writeParametersStr.toString)
        .build())
  }
}
