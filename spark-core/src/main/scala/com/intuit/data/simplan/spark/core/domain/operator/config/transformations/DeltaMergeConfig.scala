package com.intuit.data.simplan.spark.core.domain.operator.config.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorConfig

/** @author Kiran Hiremath
  */
case class DeltaMergeConfig(
    deltaTableSource: String,
    deltaEventsSource: String,
    deltaTableSourceAlias: String,
    deltaEventsSourceAlias: String,
    mergeCondition: String,
    matchCondition: Seq[Condition],
    notMatchCondition: Seq[Condition]
) extends OperatorConfig

case class Condition(expression: String, action: String)
