package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.GroupedAggregationConfig
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, expr}

class GroupedAggregationOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[GroupedAggregationConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val frame = request.dataframes(operatorConfig.source)
    val columns: List[Column] = operatorConfig.grouping.map(col)
    val listOfAggregations: List[Column] = operatorConfig.aggs.map {
      case (key, value) => expr(value) as key
    }.toList
    val relationalGroupedDataset = frame.groupBy(columns: _*)
    val aggregatedDS =
      if (listOfAggregations.size == 1) {
        relationalGroupedDataset.agg(listOfAggregations.head)
      } else
        relationalGroupedDataset.agg(listOfAggregations.head, listOfAggregations.tail: _*)

    new SparkOperatorResponse(true, Map(operatorContext.taskName -> aggregatedDS))
  }

}
