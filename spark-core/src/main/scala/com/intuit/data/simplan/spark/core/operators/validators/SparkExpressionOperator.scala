package com.intuit.data.simplan.spark.core.operators.validators

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.parsers.SparkExpressionEvaluator

/** @author Abraham, Thomas - tabraham1
  *         Created on 16-Nov-2021 at 10:58 AM
  */
case class ExpressionOperatorConfig(expression: String) extends OperatorConfig

class SparkExpressionOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[ExpressionOperatorConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val dataframes = request.dataframes
    val shouldContinue = SparkExpressionEvaluator(dataframes).evaluateBooleanExpression(operatorConfig.expression)
    SparkOperatorResponse.shouldContinue(shouldContinue)
  }
}
