package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.FilteringOperatorConfig
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 3:48 PM
  */
class FilteringOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[FilteringOperatorConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val sourceDataframe: DataFrame = request.dataframes(operatorConfig.source)
    val filteredDataframe = sourceDataframe.where(operatorConfig.condition)
    SparkOperatorResponse(operatorContext.taskName, filteredDataframe)
  }
}
