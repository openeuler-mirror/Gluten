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

import nova.hetu.omniruntime.vector.VecBatch
import nova.hetu.omniruntime.vector.serialize.VecBatchSerializerFactory
import org.apache.gluten.backendsapi.SparkPlanExecApi
import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution._
import org.apache.gluten.expression.ExpressionTransformer
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.utils.OmniAdaptorUtil.transColBatchToOmniVecs
import org.apache.spark.{ShuffleDependency, SparkException}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.shuffle.{GenShuffleWriterParameters, GlutenShuffleWriterWrapper, OmniColumnarBatchSerializer, OmniColumnarShuffleWriter, OmniShuffleUtil}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, DateDiff, Expression, Generator, GetMapValue, Like, NamedExpression, PosExplode, PythonUDF}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, BroadcastMode, Partitioning}
import org.apache.spark.sql.execution.{ColumnarShuffleExchangeExec, ColumnarWriteFilesExec, GenerateExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}
import org.apache.spark.sql.execution.joins.{BuildSideRelation, HashedRelationBroadcastMode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

class OmniSparkPlanExecApi extends SparkPlanExecApi {

  /**
   * Generate FilterExecTransformer.
   *
   * @param condition
   *   : the filter condition
   * @param child
   *   : the child of FilterExec
   * @return
   *   the transformer of FilterExec
   */
  override def genFilterExecTransformer(
      condition: Expression,
      child: SparkPlan): FilterExecTransformerBase = {
    OmniFilterExecTransformer(condition, child)
  }

  /** Generate HashAggregateExecTransformer. */
  override def genHashAggregateExecTransformer(
      requiredChildDistributionExpressions: Option[Seq[Expression]],
      groupingExpressions: Seq[NamedExpression],
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute],
      initialInputBufferOffset: Int,
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): HashAggregateExecBaseTransformer = {
    OmniHashAggregateExecTransformer(
      requiredChildDistributionExpressions,
      groupingExpressions,
      aggregateExpressions,
      aggregateAttributes,
      initialInputBufferOffset,
      resultExpressions,
      child)
  }

  /** Generate HashAggregateExecPullOutHelper */
  override def genHashAggregateExecPullOutHelper(
      aggregateExpressions: Seq[AggregateExpression],
      aggregateAttributes: Seq[Attribute]): HashAggregateExecPullOutBaseHelper =
    OmniHashAggregateExecPullOutBaseHelper(aggregateExpressions, aggregateAttributes)

  override def genColumnarShuffleExchange(shuffle: ShuffleExchangeExec): SparkPlan = {
    val child = shuffle.child
    val newShuffle = ColumnarShuffleExchangeExec(shuffle, child, shuffle.output)
    val validationResult = newShuffle.doValidate()
    if (validationResult.ok()) {
      newShuffle
    } else {
      FallbackTags.add(shuffle, validationResult)
      shuffle.withNewChildren(child :: Nil)
    }
  }

  /** Generate ShuffledHashJoinExecTransformer. */
  override def genShuffledHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean): ShuffledHashJoinExecTransformerBase = null

  /** Generate BroadcastHashJoinExecTransformer. */
  override def genBroadcastHashJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      buildSide: BuildSide,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isNullAwareAntiJoin: Boolean): BroadcastHashJoinExecTransformerBase = null

  override def genSampleExecTransformer(
      lowerBound: Double,
      upperBound: Double,
      withReplacement: Boolean,
      seed: Long,
      child: SparkPlan): SampleExecTransformer = null

  /** Generate ShuffledHashJoinExecTransformer. */
  override def genSortMergeJoinExecTransformer(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      joinType: JoinType,
      condition: Option[Expression],
      left: SparkPlan,
      right: SparkPlan,
      isSkewJoin: Boolean,
      projectList: Seq[NamedExpression]): SortMergeJoinExecTransformerBase = null

  /** Generate CartesianProductExecTransformer. */
  override def genCartesianProductExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      condition: Option[Expression]): CartesianProductExecTransformer = null

  override def genBroadcastNestedLoopJoinExecTransformer(
      left: SparkPlan,
      right: SparkPlan,
      buildSide: BuildSide,
      joinType: JoinType,
      condition: Option[Expression]): BroadcastNestedLoopJoinExecTransformer = null

  /** Generate an expression transformer to transform GetMapValue to Substrait. */
  override def genGetMapValueTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: GetMapValue): ExpressionTransformer = null

  /** Transform GetArrayItem to Substrait. */
  override def genGetArrayItemTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Expression): ExpressionTransformer = null

  /** Transform posexplode to Substrait. */
  override def genPosExplodeTransformer(
      substraitExprName: String,
      child: ExpressionTransformer,
      original: PosExplode,
      attributeSeq: Seq[Attribute]): ExpressionTransformer = null

  /**
   * Generate ShuffleDependency for ColumnarShuffleExchangeExec.
   *
   * childOutputAttributes may be different from outputAttributes, for example, the
   * childOutputAttributes include additional shuffle key columns
   *
   * @return
   */
  override def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      childOutputAttributes: Seq[Attribute],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      isSort: Boolean): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {

    // scalastyle:on argcount
    OmniShuffleUtil.genShuffleDependency(
      rdd,
      childOutputAttributes,
      newPartitioning,
      serializer,
      writeMetrics,
      metrics,
      isSort)
  }

  /** Determine whether to use sort-based shuffle based on shuffle partitioning and output. */
  override def useSortBasedShuffle(partitioning: Partitioning, output: Seq[Attribute]): Boolean =
    false

  /**
   * Generate ColumnarShuffleWriter for ColumnarShuffleManager.
   *
   * @return
   */
  override def genColumnarShuffleWriter[K, V](
      parameters: GenShuffleWriterParameters[K, V]): GlutenShuffleWriterWrapper[K, V] = {

    GlutenShuffleWriterWrapper(
      new OmniColumnarShuffleWriter[K, V](
        parameters.shuffleBlockResolver,
        parameters.columnarShuffleHandle,
        parameters.mapId,
        parameters.metrics))
  }

  /**
   * Generate ColumnarBatchSerializer for ColumnarShuffleExchangeExec.
   *
   * @return
   */
  override def createColumnarBatchSerializer(
      schema: StructType,
      metrics: Map[String, SQLMetric],
      isSort: Boolean): Serializer = {
    val readBatchNumRows = metrics("avgReadBatchNumRows")
    val numOutputRows = metrics("numOutputRows")
    val columnarConf = GlutenConfig.get
    val isRowShuffle = columnarConf.enableOmniRowShuffle &&
      schema.length > columnarConf.omniRowShuffleColumnsThreshold
    new OmniColumnarBatchSerializer(readBatchNumRows, numOutputRows, isRowShuffle)
  }

  /** Create broadcast relation for BroadcastExchangeExec */
  override def createBroadcastRelation(
      mode: BroadcastMode,
      child: SparkPlan,
      numOutputRows: SQLMetric,
      dataSize: SQLMetric): BuildSideRelation = {

    val sparkContext = child.session.sparkContext
    val nullBatchCount = sparkContext.longAccumulator("nullBatchCount")

    var nullRelationFlag = false

    val input = child
      .executeColumnar()
      .mapPartitions {
        iter =>
          val serializer = VecBatchSerializerFactory.create()
          mode match {
            case hashRelMode: HashedRelationBroadcastMode =>
              nullRelationFlag = hashRelMode.isNullAware
            case _ =>
          }
          new Iterator[Array[Byte]] {
            override def hasNext: Boolean = {
              iter.hasNext
            }

            override def next(): Array[Byte] = {
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
              numOutputRows += vecBatch.getRowCount
              val vecBatchSer = serializer.serialize(vecBatch)
              dataSize += vecBatchSer.length
              // close omni vec
              vecBatch.releaseAllVectors()
              vecBatch.close()
              vecBatchSer
            }
          }
      }
      .collect()
    val relation = OmniColumnarBuildSideRelation(mode, child.output, input)
    if (dataSize.value >= BroadcastExchangeExec.MAX_BROADCAST_TABLE_BYTES) {
      throw new SparkException(
        s"Cannot broadcast the table that is larger than 8GB: ${dataSize.value >> 30} GB")
    }
    // todo: add EmptyHashedRelation & HashedRelationWithAllNullKeys
    relation
  }

  /** Create ColumnarWriteFilesExec */
  override def createColumnarWriteFilesExec(
      child: WriteFilesExecTransformer,
      noop: SparkPlan,
      fileFormat: FileFormat,
      partitionColumns: Seq[Attribute],
      bucketSpec: Option[BucketSpec],
      options: Map[String, String],
      staticPartitions: TablePartitionSpec): ColumnarWriteFilesExec = null

  /** Create ColumnarArrowEvalPythonExec, for velox backend */
  override def createColumnarArrowEvalPythonExec(
      udfs: Seq[PythonUDF],
      resultAttrs: Seq[Attribute],
      child: SparkPlan,
      evalType: Int): SparkPlan = null

  override def genLikeTransformer(
      substraitExprName: String,
      left: ExpressionTransformer,
      right: ExpressionTransformer,
      original: Like): ExpressionTransformer = null

  override def genDateDiffTransformer(
      substraitExprName: String,
      endDate: ExpressionTransformer,
      startDate: ExpressionTransformer,
      original: DateDiff): ExpressionTransformer = null

  override def genGenerateTransformer(
      generator: Generator,
      requiredChildOutput: Seq[Attribute],
      outer: Boolean,
      generatorOutput: Seq[Attribute],
      child: SparkPlan): GenerateExecTransformerBase = null

  override def genPreProjectForGenerate(generate: GenerateExec): SparkPlan = null

  override def genPostProjectForGenerate(generate: GenerateExec): SparkPlan = null

  override def maybeCollapseTakeOrderedAndProject(plan: SparkPlan): SparkPlan = {
    // This to-top-n optimization assumes exchange operators were already placed in input plan.
    plan.transformUp {
      case p @ LimitExecTransformer(SortExecTransformer(sortOrder, _, child, _), 0, count) =>
        val global = child.outputPartitioning.satisfies(AllTuples)
        val topN = OmniTopNTransformer(count, sortOrder, global, child)
        if (topN.doValidate().ok()) {
          topN
        } else {
          p
        }
      case other => other
    }
  }
}
