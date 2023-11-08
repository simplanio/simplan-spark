package com.intuit.data.simplan.spark.core.parsers

import com.intuit.data.simplan.parser.combinators.{BaseCombinatorParser, ExpressionEvaluator}
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 21-Oct-2021 at 12:52 PM
  */
class SparkExpressionEvaluator(data: Map[String, DataFrame]) extends ExpressionEvaluator {
  override val baseCombinator: BaseCombinatorParser = new SparkExpressionParser(data)
  def evaluateBooleanExpression(expression: String): Boolean = baseCombinator.parseAll(baseCombinator.booleanOperations, expression).get
  def evaluateNumericExpression(expression: String): Double = baseCombinator.parseAll(baseCombinator.numericOperations, expression).get
}

object SparkExpressionEvaluator {
  def apply(data: Map[String, DataFrame]): SparkExpressionEvaluator = new SparkExpressionEvaluator(data)
}
