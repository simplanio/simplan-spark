package com.intuit.data.simplan.spark.core.operators.transformations

import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.lit

@CaseClassDeserialize
case class AttachConstColumnConfig(source: String, const: Map[String, String]) extends OperatorConfig

class AttachConstColumnOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[AttachConstColumnConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val sourceDataframe: DataFrame = request.dataframes(operatorConfig.source)
    val attached = operatorConfig.const.foldLeft(sourceDataframe)((a, i) => a.withColumn(i._1, lit(i._2)))
    SparkOperatorResponse(operatorContext.taskName, attached)
  }
}
