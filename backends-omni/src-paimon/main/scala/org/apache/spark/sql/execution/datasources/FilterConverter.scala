/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
 */
package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources

/** Bridges Catalyst Expression -> sources.Filter from outside the Spark package. */
object FilterConverter {
  def toSourceFilters(expressions: Seq[Expression]): Option[sources.Filter] =
    expressions
      .flatMap(DataSourceStrategy.translateFilter(_, supportNestedPredicatePushdown = true))
      .reduceOption(sources.And(_, _))
}
