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

import org.apache.gluten.extension.ValidationResult
import org.apache.gluten.metrics.MetricsUpdater
import org.apache.gluten.substrait.SubstraitContext
import org.apache.gluten.substrait.expression.ExpressionNode
import org.apache.gluten.substrait.rel.RelNode
import org.apache.spark.sql.catalyst.expressions.{Attribute, Generator}
import org.apache.spark.sql.execution.SparkPlan

case class OmniGenerateExecTransformer(
                                        generator: Generator,
                                        requiredChildOutput: Seq[Attribute],
                                        outer: Boolean,
                                        generatorOutput: Seq[Attribute],
                                        child: SparkPlan)
  extends GenerateExecTransformerBase(
    generator,
    requiredChildOutput,
    outer,
    generatorOutput,
    child) {

  override protected def withNewChildInternal(newChild: SparkPlan): OmniGenerateExecTransformer =
    copy(generator, requiredChildOutput, outer, generatorOutput, newChild)

  override protected def doGeneratorValidate(generator: Generator,
                                              outer: Boolean): ValidationResult =
    ValidationResult.failed("UnSupport GenerateExec.")

  override protected def getRelNode(context: SubstraitContext, inputRel: RelNode, generatorNode: ExpressionNode, validation: Boolean): RelNode = null

  override def metricsUpdater(): MetricsUpdater = null
}
