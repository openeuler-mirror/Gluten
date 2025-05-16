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
package org.apache.spark.shuffle

import org.apache.gluten.expression.OmniExpressionAdaptor.{getExprIdFromExpressions, getExprIdMap, isAllSimpleExpression, rewriteToOmniJsonExpressionLiteral, sparkTypeToOmniType}
import org.apache.gluten.utils.Constant.IS_SKIP_VERIFY_EXP
import org.apache.gluten.utils.OmniAdaptorUtil
import org.apache.gluten.utils.OmniAdaptorUtil.{addLeakSafeTaskCompletionListener, transColBatchToOmniVecs}
import org.apache.gluten.vectorized.{NativePartitioning, OmniColumnVector}

import org.apache.spark.{Partitioner, RangePartitioner, ShuffleDependency, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.Serializer
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, Expression, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.LazilyGeneratedOrdering
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, RangePartitioning, RoundRobinPartitioning, SinglePartition}
import org.apache.spark.sql.execution.SQLExecution
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec.createShuffleWriteProcessor
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.vectorized.ColumnarBatch
import org.apache.spark.util.MutablePair
import org.apache.spark.util.random.XORShiftRandom

import nova.hetu.omniruntime.`type`.{DataType, DataTypeSerializer}
import nova.hetu.omniruntime.operator.config.{OperatorConfig, OverflowConfig, SpillConfig}
import nova.hetu.omniruntime.operator.project.OmniProjectOperatorFactory
import nova.hetu.omniruntime.utils.ShuffleHashHelper
import nova.hetu.omniruntime.vector.{IntVec, VecBatch}

import scala.collection.JavaConverters.asScalaIteratorConverter

object OmniShuffleUtil {

  val defaultMm3HashSeed: Int = 42;
  val rollupConst: String = "spark_grouping_id"

  // scalastyle:off argcount
  def genShuffleDependency(
      rdd: RDD[ColumnarBatch],
      outputAttributes: Seq[Attribute],
      newPartitioning: Partitioning,
      serializer: Serializer,
      writeMetrics: Map[String, SQLMetric],
      metrics: Map[String, SQLMetric],
      isSort: Boolean): ShuffleDependency[Int, ColumnarBatch, ColumnarBatch] = {

    metrics("numPartitions").set(newPartitioning.numPartitions)
    val executionId = rdd.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(
      rdd.sparkContext,
      executionId,
      metrics("numPartitions") :: Nil)

    val part: Option[Partitioner] = newPartitioning match {
      case RangePartitioning(sortingExpressions, numPartitions) =>
        // Extract only fields used for sorting to avoid collecting large fields that does not
        // affect sorting result when deciding partition bounds in RangePartitioner
        val rddForSampling = rdd.mapPartitionsInternal {
          iter =>
            // Internally, RangePartitioner runs a job on the RDD that samples keys to compute
            // partition bounds. To get accurate samples, we need to copy the mutable keys.
            val projection =
              UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
            iter.flatMap(
              batch => {
                val rows: Iterator[InternalRow] = batch.rowIterator.asScala
                val mutablePair = new MutablePair[InternalRow, Null]()
                new Iterator[MutablePair[InternalRow, Null]] {
                  var closed = false

                  override def hasNext: Boolean = {
                    val has: Boolean = rows.hasNext
                    if (!has && !closed) {
                      batch.close()
                      closed = true
                    }
                    has
                  }

                  override def next(): MutablePair[InternalRow, Null] = {
                    mutablePair.update(projection(rows.next()).copy(), null)
                  }
                }
              })
        }
        // Construct ordering on extracted sort key.
        val orderingAttributes: Seq[SortOrder] = sortingExpressions.zipWithIndex.map {
          case (ord, i) =>
            ord.copy(child = BoundReference(i, ord.dataType, ord.nullable))
        }
        implicit val ordering = new LazilyGeneratedOrdering(orderingAttributes)
        val part = new RangePartitioner(
          numPartitions,
          rddForSampling,
          ascending = true,
          samplePointsPerPartitionHint = SQLConf.get.rangeExchangeSampleSizePerPartition)
        Some(part)
      case HashPartitioning(_, n) =>
        Some(new PartitionIdPassThrough(n))
      case _ => None
    }

    val inputTypes = new Array[DataType](outputAttributes.size)
    outputAttributes.zipWithIndex.foreach {
      case (attr, i) =>
        inputTypes(i) = sparkTypeToOmniType(attr.dataType, attr.metadata)
    }

    // gen RoundRobin pid
    def getRoundRobinPartitionKey: (ColumnarBatch, Int) => IntVec = {
      // 随机数
      (columnarBatch: ColumnarBatch, numPartitions: Int) =>
        {
          val pidArr = new Array[Int](columnarBatch.numRows())
          for (i <- 0 until columnarBatch.numRows()) {
            val partitionId = TaskContext.get().partitionId()
            val position = new XORShiftRandom(partitionId).nextInt(numPartitions)
            pidArr(i) = position
          }
          val vec = new IntVec(columnarBatch.numRows())
          vec.put(pidArr, 0, 0, pidArr.length)
          vec
        }
    }

    def addPidToColumnBatch(): (IntVec, ColumnarBatch) => (Int, ColumnarBatch) = (pidVec, cb) => {
      val pidVecTmp = new OmniColumnVector(cb.numRows(), IntegerType, false)
      pidVecTmp.setVec(pidVec)
      val newColumns = (pidVecTmp +: (0 until cb.numCols).map(cb.column)).toArray
      (0, new ColumnarBatch(newColumns, cb.numRows))
    }

    def computePartitionId(
        cbIter: Iterator[ColumnarBatch],
        partitionKeyExtractor: InternalRow => Any): Iterator[(Int, ColumnarBatch)] = {
      val addPid2ColumnBatch = addPidToColumnBatch()
      cbIter.filter(cb => cb.numRows != 0 && cb.numCols != 0).map {
        cb =>
          var pidVec: IntVec = null
          try {
            val pidArr = new Array[Int](cb.numRows)
            (0 until cb.numRows).foreach {
              i =>
                val row = cb.getRow(i)
                val pid = part.get.getPartition(partitionKeyExtractor(row))
                pidArr(i) = pid
            }
            pidVec = new IntVec(cb.numRows)
            pidVec.put(pidArr, 0, 0, cb.numRows)

            addPid2ColumnBatch(pidVec, cb)
          } catch {
            case e: Exception =>
              if (pidVec != null) {
                pidVec.close()
              }
              throw e
          }
      }
    }

    val isRoundRobin = newPartitioning.isInstanceOf[RoundRobinPartitioning] &&
      newPartitioning.numPartitions > 1
    val isOrderSensitive = isRoundRobin && !SQLConf.get.sortBeforeRepartition

    def containsRollUp(expressions: Seq[Expression]): Boolean = {
      expressions.exists {
        case attr: AttributeReference if rollupConst.equals(attr.name) => true
        case _ => false
      }
    }

    val rddWithPartitionId: RDD[Product2[Int, ColumnarBatch]] = newPartitioning match {
      case RoundRobinPartitioning(numPartitions) =>
        // 按随机数分区
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            val getRoundRobinPid = getRoundRobinPartitionKey
            val addPid2ColumnBatch = addPidToColumnBatch()
            cbIter.map {
              cb =>
                var pidVec: IntVec = null
                try {
                  pidVec = getRoundRobinPid(cb, numPartitions)
                  addPid2ColumnBatch(pidVec, cb)
                } catch {
                  case e: Exception =>
                    if (pidVec != null) {
                      pidVec.close()
                    }
                    throw e
                }
            }
          },
          isOrderSensitive = isOrderSensitive
        )
      case RangePartitioning(sortingExpressions, _) =>
        // 排序，按采样数据进行分区
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            val partitionKeyExtractor: InternalRow => Any = {
              val projection =
                UnsafeProjection.create(sortingExpressions.map(_.child), outputAttributes)
              row => projection(row)
            }
            val newIter = computePartitionId(cbIter, partitionKeyExtractor)
            newIter
          },
          isOrderSensitive = isOrderSensitive
        )
      case h @ HashPartitioning(expressions, numPartitions) =>
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            val addPid2ColumnBatch = addPidToColumnBatch()
            val isAllExpressionSimple = isAllSimpleExpression(expressions)
            if (isAllExpressionSimple) {
              val outputIndexMap = getExprIdMap(outputAttributes)
              val exprIds = getExprIdFromExpressions(expressions)
              cbIter.map {
                cb =>
                  val vecs = transColBatchToOmniVecs(cb)

                  val projectedVecs: Array[Long] = exprIds
                    .map(
                      exprId => {
                        vecs(outputIndexMap(exprId)).getNativeVector
                      })
                    .toArray
                  val nativeIntVecAddr =
                    ShuffleHashHelper.computePartitionIds(
                      projectedVecs,
                      numPartitions,
                      cb.numRows())
                  addPid2ColumnBatch(new IntVec(nativeIntVecAddr), cb)
              }
            } else {
              // omni project
              // todo optimize use OmniProjectOperatorFactory
              val genHashExpression = genHashExpr()
              val omniExpr: String =
                genHashExpression(expressions, numPartitions, defaultMm3HashSeed, outputAttributes)
              val factory =
                new OmniProjectOperatorFactory(
                  Array(omniExpr),
                  inputTypes,
                  1,
                  new OperatorConfig(
                    SpillConfig.NONE,
                    new OverflowConfig(OmniAdaptorUtil.overflowConf()),
                    IS_SKIP_VERIFY_EXP))
              val op = factory.createOperator()
              // close operator
              addLeakSafeTaskCompletionListener[Unit](
                _ => {
                  op.close()
                  factory.close()
                })
              cbIter.map {
                cb =>
                  var pidVec: IntVec = null
                  try {
                    val vecs = transColBatchToOmniVecs(cb, true)
                    op.addInput(new VecBatch(vecs, cb.numRows()))
                    val res = op.getOutput
                    if (res.hasNext) {
                      val retBatch = res.next()
                      pidVec = retBatch.getVectors()(0).asInstanceOf[IntVec]
                      // close return VecBatch
                      retBatch.close()
                      addPid2ColumnBatch(pidVec.asInstanceOf[IntVec], cb)
                    } else {
                      throw new Exception("Empty Project Operator Result...")
                    }
                  } catch {
                    case e: Exception =>
                      if (pidVec != null) {
                        pidVec.close()
                      }
                      throw e
                  }
              }
            }
          },
          isOrderSensitive = isOrderSensitive
        )
      case SinglePartition =>
        rdd.mapPartitionsWithIndexInternal(
          (_, cbIter) => {
            cbIter.map(cb => (0, cb))
          },
          isOrderSensitive = isOrderSensitive)
      case _ => throw new IllegalStateException(s"Exchange not implemented for $newPartitioning")
    }

    val numCols = outputAttributes.size
    val intputTypeArr: Seq[DataType] = outputAttributes.map {
      attr => sparkTypeToOmniType(attr.dataType, attr.metadata)
    }
    val intputTypes = DataTypeSerializer.serialize(intputTypeArr.toArray)

    // adapt to NativePartitioning from omni PartitionInfo
    // use schema represent numCols
    val nativePartitioning: NativePartitioning = newPartitioning match {
      case SinglePartition =>
        new NativePartitioning(
          GlutenShuffleUtils.SinglePartitioningShortName,
          1,
          new Array[Byte](numCols),
          Array.empty[Byte],
          intputTypes.getBytes)
      case RoundRobinPartitioning(n) =>
        new NativePartitioning(
          GlutenShuffleUtils.RoundRobinPartitioningShortName,
          n,
          new Array[Byte](numCols),
          Array.empty[Byte],
          intputTypes.getBytes)
      case HashPartitioning(exprs, n) =>
        new NativePartitioning(
          GlutenShuffleUtils.HashPartitioningShortName,
          n,
          new Array[Byte](numCols),
          Array.empty[Byte],
          intputTypes.getBytes)
      // range partitioning fall back to row-based partition id computation
      case RangePartitioning(orders, n) =>
        new NativePartitioning(
          GlutenShuffleUtils.RangePartitioningShortName,
          n,
          new Array[Byte](numCols),
          Array.empty[Byte],
          intputTypes.getBytes)
    }

    new ColumnarShuffleDependency[Int, ColumnarBatch, ColumnarBatch](
      rddWithPartitionId,
      new PartitionIdPassThrough(newPartitioning.numPartitions),
      serializer,
      shuffleWriterProcessor = createShuffleWriteProcessor(writeMetrics),
      nativePartitioning = nativePartitioning,
      metrics = metrics,
      isSort = isSort
    )
  }

  // gen hash partition expression
  def genHashExpr(): (Seq[Expression], Int, Int, Seq[Attribute]) => String = {
    (
        expressions: Seq[Expression],
        numPartitions: Int,
        seed: Int,
        outputAttributes: Seq[Attribute]) =>
      {
        val exprIdMap = getExprIdMap(outputAttributes)
        val EXP_JSON_FORMATER1 =
          ("{\"exprType\":\"FUNCTION\",\"returnType\":1,\"function_name\":\"%s\",\"arguments\":[" +
            "%s,{\"exprType\":\"LITERAL\",\"dataType\":1,\"isNull\":false,\"value\":%d}]}")
        val EXP_JSON_FORMATER2 =
          ("{\"exprType\": \"FUNCTION\",\"returnType\":1,\"function_name\":\"%s\", \"arguments\": " +
            "[%s,%s] }")
        var omniExpr: String = ""
        expressions.foreach {
          expr =>
            val colExpr = rewriteToOmniJsonExpressionLiteral(expr, exprIdMap)
            if (omniExpr.isEmpty) {
              omniExpr = EXP_JSON_FORMATER1.format("mm3hash", colExpr, seed)
            } else {
              omniExpr = EXP_JSON_FORMATER2.format("mm3hash", colExpr, omniExpr)
            }
        }
        omniExpr = EXP_JSON_FORMATER1.format("pmod", omniExpr, numPartitions)
//      logDebug(s"hash omni expression: $omniExpr")
        omniExpr
      }
  }
}

private[spark] class PartitionIdPassThrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
