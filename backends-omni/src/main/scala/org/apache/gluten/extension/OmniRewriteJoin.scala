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

import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.joins.{ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.internal.SQLConf

case class OmniRewriteJoin() extends Rule[SparkPlan] with JoinSelectionHelper {

    private def canBuildLocalHashMapBySize(
        plan: LogicalPlan,
        conf: SQLConf): Boolean = {
        plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions

        val threshold = conf.autoBroadcastJoinThreshold
        val partitions = conf.numShufflePartitions
        val size = plan.stats.sizeInBytes

        threshold > 0 &&
          partitions > 0 &&
          size >= 0 &&
          size < BigInt(threshold) * BigInt(partitions)
    }

    private def getOmniBuildSide(
                              canBuildLeft: Boolean,
                              canBuildRight: Boolean,
                              left: LogicalPlan,
                              right: LogicalPlan): Option[BuildSide] = {
        if (canBuildLeft && canBuildRight) {
            // returns the smaller side base on its estimated physical size, if we want to build the
            // both sides.
            Some(getSmallerSide(left, right))
        } else if (canBuildLeft) {
            Some(BuildLeft)
        } else if (canBuildRight) {
            Some(BuildRight)
        } else {
            None
        }
    }

    private def muchSmaller(
        smaller: LogicalPlan,
        larger: LogicalPlan,
        conf: SQLConf): Boolean = {

        val factor = conf.getConf(SQLConf.SHUFFLE_HASH_JOIN_FACTOR)
        val smallerSize = smaller.stats.sizeInBytes
        val largerSize = larger.stats.sizeInBytes

        factor > 0 &&
          smallerSize >= 0 &&
          largerSize >= 0 &&
          smallerSize * BigInt(factor) <= largerSize
    }

    private def getOmniShuffleHashJoinBuildSide(
        left: LogicalPlan,
        right: LogicalPlan,
        joinType: org.apache.spark.sql.catalyst.plans.JoinType,
        hint: JoinHint,
        hintOnly: Boolean,
        conf: SQLConf,
        forceShuffledHashJoin: Boolean): Option[BuildSide] = {

        val buildLeft =
            if (hintOnly) {
                hintToShuffleHashJoinLeft(hint)
            } else {
                hintToPreferShuffleHashJoinLeft(hint) ||
                  (
                    (forceShuffledHashJoin || !conf.preferSortMergeJoin) &&
                      canBuildLocalHashMapBySize(left, conf) &&
                      muchSmaller(left, right, conf)
                    )
            }

        val buildRight =
            if (hintOnly) {
                hintToShuffleHashJoinRight(hint)
            } else {
                hintToPreferShuffleHashJoinRight(hint) ||
                  (
                    (forceShuffledHashJoin || !conf.preferSortMergeJoin) &&
                      canBuildLocalHashMapBySize(right, conf) &&
                      muchSmaller(right, left, conf)
                    )
            }

        getOmniBuildSide(
            canBuildShuffledHashJoinLeft(joinType) && buildLeft,
            canBuildShuffledHashJoinRight(joinType) && buildRight,
            left,
            right)
    }

    private def getShjBuildSide(smj: SortMergeJoinExec): Option[BuildSide] = {
        val config = GlutenConfig.get
        val conf = SQLConf.get

        smj.logicalLink match {
            case Some(join: Join) =>
                val hint = join.hint

                if (hintToSortMergeJoin(hint)) {
                    return None
                }

                val hintedBuildSide =
                    getOmniShuffleHashJoinBuildSide(
                        join.left,
                        join.right,
                        join.joinType,
                        hint,
                        hintOnly = true,
                        conf,
                        false)

                if (hintedBuildSide.isDefined) {
                    return hintedBuildSide
                }


                getOmniShuffleHashJoinBuildSide(
                    join.left,
                    join.right,
                    join.joinType,
                    hint,
                    hintOnly = false,
                    conf,
                    config.forceShuffledHashJoin)

            case _ =>
                None
        }
    }

      private def rewriteJoin(
                               smj: SortMergeJoinExec,
                               buildSide: BuildSide): SparkPlan = {
          val shj = ShuffledHashJoinExec(
              smj.leftKeys,
              smj.rightKeys,
              smj.joinType,
              buildSide,
              smj.condition,
              smj.left,
              smj.right,
              smj.isSkewJoin)

          shj.copyTagsFrom(smj)
          smj.logicalLink.foreach(shj.setLogicalLink)
          shj
      }

    override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
        case smj: SortMergeJoinExec =>
            getShjBuildSide(smj)
              .map(buildSide => rewriteJoin(smj, buildSide))
              .getOrElse(smj)
    }
}