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

import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import org.apache.gluten.execution.OmniSerializerResult
import org.apache.gluten.utils.OmniAdaptorUtil.transColBatchToOmniVecs
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.physical.BroadcastMode
import org.apache.spark.sql.execution.joins.HashedRelationBroadcastMode


object OmniExecUtil extends Logging {

  def buildSideRDD(
                    mode: BroadcastMode,
                    newChild: SparkPlan): RDD[OmniSerializerResult] = {
    val sparkContext = newChild.session.sparkContext
    val nullBatchCount = sparkContext.longAccumulator("nullBatchCount")
    var nullRelationFlag = false
    val serializer = VecBatchSerializerFactory.create()
    mode match {
      case hashRelMode: HashedRelationBroadcastMode =>
         nullRelationFlag = hashRelMode.isNullAware
      case _ =>
    }
    newChild
      .executeColumnar()
      .mapPartitionsInternal {
        iter =>
          new Iterator[OmniSerializerResult] {
            override def hasNext: Boolean = {
              iter.hasNext
            }

            override def next(): OmniSerializerResult = {
              val batch = iter.next()
              var index = 0
              // When nullRelationFlag is true, it means anti-join
              // Only one column of data is involved in the anti-
              if (nullRelationFlag && batch.numCols() > 0) {
                val vec = batch.column(0)
                if (vec.hasNull) {
                  try {
                    nullBatchCount.add(1)
                  } catch {
                    case e: Exception =>
                      throw new SparkException(s"compute null BatchCount error : ${e.getMessage}.")
                  }
                }
              }
              val vectors = transColBatchToOmniVecs(batch)
              val vecBatch = new VecBatch(vectors, batch.numRows())

              val vecBatchSer = serializer.serialize(vecBatch)

              val result = OmniSerializerResult(vecBatch.getRowCount, vecBatchSer)
              // close omni vec
              vecBatch.releaseAllVectors()
              vecBatch.close()
              result
            }
          }
      }
  }
}
