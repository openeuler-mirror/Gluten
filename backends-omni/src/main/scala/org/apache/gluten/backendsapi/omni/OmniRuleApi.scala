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

import org.apache.gluten.backendsapi.RuleApi
import org.apache.gluten.columnarbatch.OmniBatch
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.extension.columnar._
import org.apache.gluten.extension.columnar.MiscColumnarRules.{RemoveGlutenTableCacheColumnarToRow, RemoveTopmostColumnarToRow, RewriteSubqueryBroadcast}
import org.apache.gluten.extension.columnar.heuristic.{ExpandFallbackPolicy, HeuristicTransform}
import org.apache.gluten.extension.columnar.offload.{OffloadExchange, OffloadJoin, OffloadOthers}
import org.apache.gluten.extension.columnar.rewrite._
import org.apache.gluten.extension.columnar.transition.{InsertTransitions, RemoveTransitions}
import org.apache.gluten.extension.columnar.validator.{Validator, Validators}
import org.apache.gluten.extension.injector.{Injector, SparkInjector}
import org.apache.gluten.extension.injector.GlutenInjector.{LegacyInjector, RasInjector}
import org.apache.gluten.extension.RewriteAQEShuffleRead
import org.apache.spark.sql.catalyst.optimizer.ReorderJoinEnhances
import org.apache.gluten.extension.{FallbackBroadcastHashJoin, FallbackBroadcastHashJoinPrepQueryStage, RewriteAQEShuffleRead}
import org.apache.spark.sql.execution.{ColumnarCollapseTransformStages, GlutenFallbackReporter}

class OmniRuleApi extends RuleApi {

  import OmniRuleApi._

  override def injectRules(injector: Injector): Unit = {
    injectSpark(injector.spark)
    injectLegacy(injector.gluten.legacy)
    injectRas(injector.gluten.ras)
  }
}

object OmniRuleApi {
  private def injectSpark(injector: SparkInjector): Unit = {
    // Inject the regular Spark rules directly.
    injector.injectQueryStagePrepRule(FallbackBroadcastHashJoinPrepQueryStage.apply)
//    injector.injectOptimizerRule(CollectRewriteRule.apply)
//    injector.injectOptimizerRule(HLLRewriteRule.apply)
//    injector.injectOptimizerRule(CollapseGetJsonObjectExpressionRule.apply)
//    injector.injectPostHocResolutionRule(ArrowConvertorRule.apply)
    injector.injectOptimizerRule(ReorderJoinEnhances.apply)
  }

  private def injectLegacy(injector: LegacyInjector): Unit = {
    // Legacy: Pre-transform rules.
    injector.injectPreTransform(_ => RemoveTransitions)
    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
    injector.injectPreTransform(c => FallbackMultiCodegens.apply(c.session))
    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
    injector.injectPreTransform(_ => RewriteAQEShuffleRead())
    injector.injectPreTransform(c => FallbackBroadcastHashJoin.apply(c.session))
//    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
//    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))

    // Legacy: The legacy transform rule.
    val offloads = Seq(OffloadOthers(), OffloadExchange(), OffloadJoin())
    val validatorBuilder: GlutenConfig => Validator = conf =>
      Validators.newValidator(conf, offloads)
    val rewrites =
      Seq(
        RewriteIn,
        RewriteMultiChildrenCount,
        RewriteJoin,
        PullOutPreProject,
        PullOutPostProject,
        ProjectColumnPruning)
    injector.injectTransform(
      c => HeuristicTransform.WithRewrites(validatorBuilder(c.glutenConf), rewrites, offloads))

    // Legacy: Post-transform rules.
    injector.injectPostTransform(_ => UnionTransformerRule())
//    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
    injector.injectPostTransform(_ => PushDownFilterToScan)
    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
    injector.injectPostTransform(_ => EliminateLocalSort)
    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
//    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, OmniBatch))

    // Gluten columnar: Fallback policies.
    injector.injectFallbackPolicy(
      c => ExpandFallbackPolicy(c.ac.isAdaptiveContext(), c.ac.originalPlan()))

    // Gluten columnar: Post rules.
    injector.injectPost(c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
//    SparkShimLoader.getSparkShims
//      .getExtendedColumnarPostRules()
//      .foreach(each => injector.injectPost(c => each(c.session)))
    injector.injectPost(c => ColumnarCollapseTransformStages(c.glutenConf))

    // Gluten columnar: Final rules.
    injector.injectFinal(c => RemoveGlutenTableCacheColumnarToRow(c.session))
    injector.injectFinal(c => GlutenFallbackReporter(c.glutenConf, c.session))
    injector.injectFinal(_ => RemoveFallbackTagRule())
  }

  private def injectRas(injector: RasInjector): Unit = {
    // Gluten RAS: Pre rules.
//    injector.injectPreTransform(_ => RemoveTransitions)
//    injector.injectPreTransform(_ => PushDownInputFileExpression.PreOffload)
//    injector.injectPreTransform(c => FallbackOnANSIMode.apply(c.session))
//    injector.injectPreTransform(c => MergeTwoPhasesHashBaseAggregate(c.session))
//    injector.injectPreTransform(_ => RewriteSubqueryBroadcast())
//    injector.injectPreTransform(c => BloomFilterMightContainJointRewriteRule.apply(c.session))
//    injector.injectPreTransform(c => ArrowScanReplaceRule.apply(c.session))
//
//    // Gluten RAS: The RAS rule.
//    val validatorBuilder: GlutenConfig => Validator = conf => Validators.newValidator(conf)
//    val rewrites =
//      Seq(
//        RewriteIn,
//        RewriteMultiChildrenCount,
//        RewriteJoin,
//        PullOutPreProject,
//        PullOutPostProject,
//        ProjectColumnPruning)
//    injector.injectCoster(_ => LegacyCoster)
//    injector.injectCoster(_ => RoughCoster)
//    injector.injectCoster(_ => RoughCoster2)
//    injector.injectRasRule(_ => RemoveSort)
//    val offloads: Seq[RasOffload] = Seq(
//      RasOffload.from[Exchange](OffloadExchange()),
//      RasOffload.from[BaseJoinExec](OffloadJoin()),
//      RasOffload.from[FilterExec](OffloadOthers()),
//      RasOffload.from[ProjectExec](OffloadOthers()),
//      RasOffload.from[DataSourceV2ScanExecBase](OffloadOthers()),
//      RasOffload.from[DataSourceScanExec](OffloadOthers()),
//      RasOffload.from(HiveTableScanExecTransformer.isHiveTableScan(_))(OffloadOthers()),
//      RasOffload.from[CoalesceExec](OffloadOthers()),
//      RasOffload.from[HashAggregateExec](OffloadOthers()),
//      RasOffload.from[SortAggregateExec](OffloadOthers()),
//      RasOffload.from[ObjectHashAggregateExec](OffloadOthers()),
//      RasOffload.from[UnionExec](OffloadOthers()),
//      RasOffload.from[ExpandExec](OffloadOthers()),
//      RasOffload.from[WriteFilesExec](OffloadOthers()),
//      RasOffload.from[SortExec](OffloadOthers()),
//      RasOffload.from[TakeOrderedAndProjectExec](OffloadOthers()),
//      RasOffload.from[WindowExec](OffloadOthers()),
//      RasOffload.from(SparkShimLoader.getSparkShims.isWindowGroupLimitExec(_))(OffloadOthers()),
//      RasOffload.from[LimitExec](OffloadOthers()),
//      RasOffload.from[GenerateExec](OffloadOthers()),
//      RasOffload.from[EvalPythonExec](OffloadOthers()),
//      RasOffload.from[SampleExec](OffloadOthers())
//    )
//    offloads.foreach(
//      offload =>
//        injector.injectRasRule(
//          c => RasOffload.Rule(offload, validatorBuilder(c.glutenConf), rewrites)))
//
//    // Gluten RAS: Post rules.
//    injector.injectPostTransform(_ => RemoveTransitions)
//    injector.injectPostTransform(_ => UnionTransformerRule())
//    injector.injectPostTransform(c => PartialProjectRule.apply(c.session))
//    injector.injectPostTransform(_ => RemoveNativeWriteFilesSortAndProject())
//    injector.injectPostTransform(_ => PushDownFilterToScan)
//    injector.injectPostTransform(_ => PushDownInputFileExpression.PostOffload)
//    injector.injectPostTransform(_ => EnsureLocalSortRequirements)
//    injector.injectPostTransform(_ => EliminateLocalSort)
//    injector.injectPostTransform(_ => CollapseProjectExecTransformer)
//    injector.injectPostTransform(c => FlushableHashAggregateRule.apply(c.session))
//    injector.injectPostTransform(c => InsertTransitions.create(c.outputsColumnar, VeloxBatch))
//    injector.injectPostTransform(
//      c => RemoveTopmostColumnarToRow(c.session, c.ac.isAdaptiveContext()))
//    SparkShimLoader.getSparkShims
//      .getExtendedColumnarPostRules()
//      .foreach(each => injector.injectPostTransform(c => each(c.session)))
//    injector.injectPostTransform(c => ColumnarCollapseTransformStages(c.glutenConf))
//    injector.injectPostTransform(c => RemoveGlutenTableCacheColumnarToRow(c.session))
//    injector.injectPostTransform(c => GlutenFallbackReporter(c.glutenConf, c.session))
//    injector.injectPostTransform(_ => RemoveFallbackTagRule())
  }

}
