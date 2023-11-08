package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.JoinOperatorConfig
import com.intuit.data.simplan.parser.combinators.BaseCombinatorParser
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Nov-2021 at 4:28 PM
  */
class JoinOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[JoinOperatorConfig](appContext, operatorContext) {
  private val evaluator: BaseCombinatorParser = new BaseCombinatorParser

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val leftTable = request.dataframes(operatorConfig.leftTable)
    val rightTable = request.dataframes(operatorConfig.rightTable)
    val joinType = operatorConfig.joinType
    val parsed = evaluator.parseAll(evaluator.splitOperators, operatorConfig.joinCondition)
    val (leftCol, comparator, rightCol) = parsed.get
    val joinedDF =
      if (leftCol == rightCol) {
        leftTable.join(rightTable, Seq(rightCol), joinType)
      } else {
        val joinCondition = comparator match {
          case "==" => leftTable(leftCol) === rightTable(rightCol)
        }
        leftTable.join(rightTable, joinCondition, joinType.toLowerCase)
      }
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> joinedDF))

  }
}
