package com.intuit.data.simplan.spark.core.operators.sinks.batch

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkBatchSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractBatchSink
import org.apache.spark.sql.functions.{struct, to_json}
import org.apache.spark.sql.{DataFrame, SaveMode}

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 10:58 AM
  */
class KafkaBatchSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractBatchSink(sparkAppContext, operatorContext, Map.empty, SaveMode.Append) {
  override def customizeSourceConfig(readerConfig: SparkBatchSinkConfig): SparkBatchSinkConfig = readerConfig.copy(format = "kafka")

  override def customiseFinalTransformation(dataFrame: DataFrame): DataFrame =
    dataFrame.select(to_json(struct("*")).as("value"))
      .selectExpr("CAST(value AS STRING)")
}
