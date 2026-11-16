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

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.util.SparkVersionUtil

import org.scalatest.funsuite.AnyFunSuite

class OmniTopKOffsetSuite extends AnyFunSuite {
  test("TakeOrderedAndProject applies OFFSET to the globally ordered rows") {
    assume(SparkVersionUtil.compareMajorMinorVersion(SparkVersionUtil.majorMinorVersion(), (3, 4)) >= 0)

    val conf = new SparkConf()
    assume(conf.contains(GlutenConfig.GLUTEN_LIB_PATH), "Omni native library is required")

    conf
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "512m")
      .set("spark.sql.adaptive.enabled", "false")
      .set("spark.ui.enabled", "false")
      .set(GlutenConfig.RAS_ENABLED.key, "false")

    val spark = SparkSession.builder().config(conf).getOrCreate()
    try {
      val cases = Seq(
        (1, 1, "DESC", Seq(Row(110L), Row(109L))),
        (4, 1, "DESC", Seq(Row(110L), Row(109L))),
        (4, 0, "DESC", Seq(Row(111L), Row(110L))),
        (4, 11, "DESC", Seq(Row(100L))),
        (1, 2, "ASC", Seq(Row(102L), Row(103L))))

      cases.foreach {
        case (partitions, offset, direction, expected) =>
          val sql =
            s"""SELECT id + 100 AS projected FROM range(0, 12, 1, $partitions)
               |ORDER BY id $direction LIMIT 2 OFFSET $offset""".stripMargin
          withClue(sql) {
            val df = spark.sql(sql)
            val plan = df.queryExecution.executedPlan
            assert(plan.collect {
              case topK: TakeOrderedAndProjectExecTransformer => topK.offset
            } == Seq(offset), plan.toString())
            assert(df.collect().toSeq == expected)
          }
      }
    } finally {
      spark.stop()
    }
  }
}
