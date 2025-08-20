/*
 * Copyright (C) 2025-2025. Huawei Technologies Co., Ltd. All rights reserved.
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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.gluten.execution._
import org.apache.gluten.expression.OmniExpressionAdaptor.isSimpleProjectForAll
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan

/**
 * combine join and project, only support inner join with simple project like col{0,1,2,3} to col{2,3}
 */
case class CombineJoinProject ()
  extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p @ ProjectExecTransformer(_, child: HashJoinLikeExecTransformer) =>
        child match {
          case child: ShuffledHashJoinExecTransformerBase =>
            if (p.projectList.forall(e => isSimpleProjectForAll(e)) && child.joinType == Inner) {
              val projectList: collection.immutable.Seq[NamedExpression] = p.projectList.to[collection.immutable.Seq]
              val join = OmniShuffledHashJoinExecTransformer(child.leftKeys.to[collection.immutable.Seq],
                child.rightKeys.to[collection.immutable.Seq],
                child.joinType,
                child.joinBuildSide,
                child.condition,
                child.left,
                child.right,
                child.isSkewJoin,
                projectList)
              join
            } else {
              p
            }
          case child: BroadcastHashJoinExecTransformerBase =>
            if (p.projectList.forall(e => isSimpleProjectForAll(e)) && child.joinType == Inner) {
              val projectList: collection.immutable.Seq[NamedExpression] = p.projectList.to[collection.immutable.Seq]
              val join =
              OmniBroadcastHashJoinExecTransformer(child.leftKeys.to[collection.immutable.Seq],
                child.rightKeys.to[collection.immutable.Seq],
                child.joinType,
                child.joinBuildSide,
                child.condition,
                child.left,
                child.right,
                child.genJoinParametersInternal()._2 == 1,
                projectList)
                join
            } else {
              p
            }
          case _ => p
        }
    }
  }
}
