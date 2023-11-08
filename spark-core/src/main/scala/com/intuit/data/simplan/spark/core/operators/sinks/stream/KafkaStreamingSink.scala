package com.intuit.data.simplan.spark.core.operators.sinks.stream

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkStreamingSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractStreamingSink
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Sep-2021 at 3:55 PM
  */
class KafkaStreamingSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractStreamingSink(sparkAppContext, operatorContext) {
  override def customizeConfig(readerConfig: SparkStreamingSinkConfig): SparkStreamingSinkConfig = readerConfig.copy(format = "kafka")

  override def customiseSinkTransformation(dataFrame: DataFrame): DataFrame =
    dataFrame.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
}
