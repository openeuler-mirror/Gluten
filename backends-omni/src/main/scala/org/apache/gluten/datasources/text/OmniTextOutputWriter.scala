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

import com.huawei.boostkit.spark.jni.TextColumnarBatchWriter
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.datasources.{FakeRow, OutputWriter}
import org.apache.spark.sql.types.StructType

import java.{util => ju}

class OmniTextOutputWriter(
    outputPath: String,
    dataSchema: StructType,
    context: TaskAttemptContext,
    nativeConf: ju.Map[String, String])
  extends OutputWriter {

  private val writer = new TextColumnarBatchWriter()
  private var dataColumnIds: Array[Boolean] = new Array[Boolean](0)

  def initialize(allColumns: Seq[Attribute], dataColumns: Seq[Attribute]): Unit = {
    require(dataColumns.nonEmpty, "Native Text writer requires at least one data column")
    require(
      dataSchema.length == dataColumns.length,
      "Native Text writer schema does not match its data columns")
    writer.initializeWriterJava(new Path(outputPath), dataSchema, nativeConf)
    dataColumnIds = allColumns.map(dataColumns.contains).toArray
  }

  override def write(row: InternalRow): Unit = {
    require(row.isInstanceOf[FakeRow], "Native Text writer requires FakeRow input")
    writer.write(dataColumnIds, row.asInstanceOf[FakeRow].batch)
  }

  def splitWrite(row: InternalRow, startPos: Long, endPos: Long): Unit = {
    require(row.isInstanceOf[FakeRow], "Native Text writer requires FakeRow input")
    writer.write(dataColumnIds, row.asInstanceOf[FakeRow].batch, startPos, endPos)
  }

  override def close(): Unit = writer.close()

  override def path(): String = outputPath
}
