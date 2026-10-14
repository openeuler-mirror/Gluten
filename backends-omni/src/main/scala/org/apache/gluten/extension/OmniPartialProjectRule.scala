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

import org.apache.gluten.execution.{
  OmniColumnarPartialProjectExec,
  OmniPartialProjectSplit,
  ProjectExecTransformer
}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}

case class OmniPartialProjectRule(session: SparkSession) extends Rule[SparkPlan] with Logging {

  override def apply(plan: SparkPlan): SparkPlan = {
    plan.transformUp {
      case p: ProjectExec if OmniPartialProjectSplit.projectListNeedsPartialSplit(p.projectList) =>
        if (p.child.isInstanceOf[OmniColumnarPartialProjectExec]) {
          logWarning(
            "Omni partial project skip: child is already OmniColumnarPartialProjectExec. " +
              s"projectList=${p.projectList.mkString("[", ", ", "]")}")
          p
        } else {
          val partial = OmniColumnarPartialProjectExec.create(p)
          val transformer = partial.child.asInstanceOf[ProjectExecTransformer]
          val projectValidation = transformer.doValidate()
          val partialValidation = partial.doValidate()
          if (
            projectValidation.ok() &&
            partialValidation.ok()
          ) {
            logWarning(
              "Omni partial project applied. " +
                s"childOutput=${p.child.output.mkString("[", ", ", "]")}, " +
                s"projectList=${p.projectList.mkString("[", ", ", "]")}")
            partial
          } else {
            logWarning(
              "Omni partial project fallback. " +
                s"projectValidation=${reasonOf(projectValidation)}, " +
                s"partialValidation=${reasonOf(partialValidation)}, " +
                s"childOutput=${p.child.output.mkString("[", ", ", "]")}, " +
                s"projectList=${p.projectList.mkString("[", ", ", "]")}, " +
                s"nativeProjectList=${transformer.projectList.mkString("[", ", ", "]")}, " +
                s"perExprValidation=${perExprValidation(transformer)}, " +
                s"prefixValidation=${prefixValidation(transformer)}")
            p
          }
        }
      case o => o
    }
  }

  private def reasonOf(result: ValidationResult): String =
    if (result.ok()) "OK" else result.reason()

  private def perExprValidation(transformer: ProjectExecTransformer): String = {
    transformer.projectList.zipWithIndex
      .map {
        case (expr, index) =>
          val result = ProjectExecTransformer(Seq(expr), transformer.child).doValidate()
          s"#$index ${expr.name}:${reasonOf(result)}"
      }
      .mkString("[", "; ", "]")
  }

  private def prefixValidation(transformer: ProjectExecTransformer): String = {
    transformer.projectList.indices
      .map {
        index =>
          val exprs = transformer.projectList.take(index + 1)
          val result = ProjectExecTransformer(exprs, transformer.child).doValidate()
          s"#0-$index:${reasonOf(result)}"
      }
      .mkString("[", "; ", "]")
  }
}
