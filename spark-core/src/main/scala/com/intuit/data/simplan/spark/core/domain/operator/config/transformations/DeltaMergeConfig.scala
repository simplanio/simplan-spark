package com.intuit.data.simplan.spark.core.domain.operator.config.transformations

import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
import com.intuit.data.simplan.core.domain.operator.OperatorConfig

/** @author Kiran Hiremath
  */
@CaseClassDeserialize
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
