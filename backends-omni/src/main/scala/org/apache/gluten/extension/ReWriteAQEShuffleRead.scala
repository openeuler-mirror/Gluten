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
package org.apache.gluten.extension

import org.apache.gluten.config.GlutenConfig
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AQEShuffleReadExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec

case class RewriteAQEShuffleRead() extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan.transform {
        case plan: AQEShuffleReadExec if GlutenConfig.get.enableColumnarAQEShuffle =>
          plan.child match {
          case shuffle: OmniColumnarShuffleExchangeExec =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              OmniAQEShuffleReadExec(plan.child, plan.partitionSpecs)
          case ShuffleQueryStageExec(_, shuffle: OmniColumnarShuffleExchangeExec, _) =>
              logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
              OmniAQEShuffleReadExec(plan.child, plan.partitionSpecs)
          case ShuffleQueryStageExec(_, reused: ReusedExchangeExec, _) =>
              reused match {
              case ReusedExchangeExec(_, shuffle: OmniColumnarShuffleExchangeExec) =>
                  logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
                  OmniAQEShuffleReadExec(
                  plan.child,
                  plan.partitionSpecs)
              case _ =>
                  plan
              }
          case _ =>
              plan
          }
    }
}