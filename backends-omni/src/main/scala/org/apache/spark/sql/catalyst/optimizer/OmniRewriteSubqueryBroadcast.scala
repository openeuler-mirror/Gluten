package org.apache.spark.sql.catalyst.optimizer

import org.apache.gluten.backendsapi.BackendsApiManager
import org.apache.gluten.extension.columnar.transition.Transitions
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarBroadcastExchangeExec, ColumnarSubqueryBroadcastExec, ExecSubqueryExpression, ReusedSubqueryExec, SparkPlan, SubqueryBroadcastExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.exchange.BroadcastExchangeExec
import org.apache.spark.sql.internal.SQLConf

case class OmniRewriteSubqueryBroadcast() extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    val out = plan.transformWithSubqueries {
      case p =>
        // When AQE is on, the AQE sub-query cache should already be filled with
        // row-based SubqueryBroadcastExec for reusing. Thus we are doing the same
        // memorize-and-reuse work here for the replaced columnar version.
        val reuseRemoved = removeReuses(p)
        val replaced = replace(reuseRemoved)
        replaced
    }
    out
  }

  private def removeReuses(p: SparkPlan): SparkPlan = {
    val out = p.transformExpressions {
      case pe: ExecSubqueryExpression =>
        val newPlan = pe.plan match {
          case ReusedSubqueryExec(s: SubqueryBroadcastExec) =>
            // Remove ReusedSubqueryExec. We will re-create reuses in subsequent method
            // #replace.
            //
            // We assume only meeting reused sub-queries in AQE execution. When AQE is off,
            // Spark adds reuses only after applying columnar rules by preparation rule
            // ReuseExchangeAndSubquery.
            assert(s.child.isInstanceOf[AdaptiveSparkPlanExec])
            s
          case other =>
            other
        }
        pe.withNewPlan(newPlan)
    }
    out
  }

  private def replace(p: SparkPlan): SparkPlan = {
    val out = p.transformExpressions {
      case pe: ExecSubqueryExpression =>
        val newPlan = pe.plan match {
          case s: SubqueryBroadcastExec =>
            val columnarSubqueryBroadcast = toColumnarSubqueryBroadcast(s)
            val maybeReused = columnarSubqueryBroadcast.child match {
              case a: AdaptiveSparkPlanExec if SQLConf.get.subqueryReuseEnabled =>
                val canonicalKey = columnarSubqueryBroadcast.canonicalized
                a.context.subqueryCache.get(canonicalKey) match {
                  case Some(cached) => ReusedSubqueryExec(cached)
                  case None =>
                    a.context.subqueryCache.update(canonicalKey, columnarSubqueryBroadcast)
                    columnarSubqueryBroadcast
                }
              case _ =>
                s.child match {
                  case _: ColumnarBroadcastExchangeExec => columnarSubqueryBroadcast
                  case _ => s
                }
            }
            maybeReused
          case other => other
        }
        pe.withNewPlan(newPlan)
    }
    out
  }

  private def toColumnarBroadcastExchange(
                                           exchange: BroadcastExchangeExec): ColumnarBroadcastExchangeExec = {
    val newChild =
      Transitions.toBatchPlan(exchange.child, BackendsApiManager.getSettings.primaryBatchType)
    ColumnarBroadcastExchangeExec(exchange.mode, newChild)
  }

  private def toColumnarSubqueryBroadcast(
                                           from: SubqueryBroadcastExec): ColumnarSubqueryBroadcastExec = {
    val newChild = from.child match {
      case exchange: BroadcastExchangeExec =>
        toColumnarBroadcastExchange(exchange)
      case aqe: AdaptiveSparkPlanExec =>
        // Keeps the child if its is AQE even if its supportsColumnar == false.
        // ColumnarSubqueryBroadcastExec is compatible with both row-based
        // and columnar inputs.
        aqe
      case other => other
    }
    val out = ColumnarSubqueryBroadcastExec(from.name, from.index, from.buildKeys, newChild)
    out
  }
}
