package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.ProjectionOperatorConfig
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.DataFrame

class ProjectionOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[ProjectionOperatorConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val sourceDataframe: DataFrame = request.dataframes(operatorConfig.source)
    val projectedDataframe = sourceDataframe.selectExpr(operatorConfig.projections.map(projection => projection): _*)
    SparkOperatorResponse(operatorContext.taskName, projectedDataframe)
  }
}
