package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.DropDuplicatesConfig
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator

/** @author Kiran Hiremath
  */
class DropDuplicatesOperator(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[DropDuplicatesConfig](sparkAppContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val dataframe = request.dataframes(operatorConfig.source)

    val df = dataframe
      .withWatermark(operatorConfig.eventTimeField, operatorConfig.dropWindowDuration)
      .dropDuplicates(operatorConfig.primaryKeyColumns.head, operatorConfig.primaryKeyColumns: _*)

    new SparkOperatorResponse(true, Map(operatorContext.taskName -> df))
  }

}
