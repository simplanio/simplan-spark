package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.{KVProjectionOperatorConfig, ProjectionOperatorConfig}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

class KVProjectionOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[KVProjectionOperatorConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val sourceDataframe: DataFrame = request.dataframes(operatorConfig.source)
    val columns = operatorConfig.projections.map(projection => s"${projection._2} as ${projection._1}").toList
    val projectedDataframe = sourceDataframe.selectExpr(columns: _*)
    SparkOperatorResponse(operatorContext.taskName, projectedDataframe)
  }
}
