/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.gluten.extension

import org.apache.gluten.execution.{FilterExecTransformer, OmniPaimonScanExecTransformer}

import org.apache.spark.sql.catalyst.expressions.{And, PredicateHelper}
import org.apache.spark.sql.execution.SparkPlan

object PushDownFilterToOmniPaimonScan extends PredicateHelper {
  def tryPushDown(plan: SparkPlan): Option[SparkPlan] = plan match {
    case filter: FilterExecTransformer =>
      filter.child match {
        case scan: OmniPaimonScanExecTransformer =>
          val conjuncts = splitConjunctivePredicates(filter.cond)
          val pushedFilters = PushDownFilterToOmniScan.getPushedFilter(conjuncts)
          if (pushedFilters.isEmpty) {
            None
          } else {
            val newScan = scan.copy()
            newScan.setPushDownFilters(pushedFilters)
            if (newScan.doValidate().ok()) {
              val newFilterConditions = conjuncts.filterNot(pushedFilters.toSet.contains)
              Some(
                newFilterConditions.reduceOption(And) match {
                  case Some(condition) => filter.makeCopy(Array(condition, newScan))
                  case None => newScan
                })
            } else {
              None
            }
          }
        case _ => None
      }
    case _ => None
  }
}
