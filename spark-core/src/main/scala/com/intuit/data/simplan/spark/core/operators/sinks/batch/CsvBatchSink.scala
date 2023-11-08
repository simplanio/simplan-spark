package com.intuit.data.simplan.spark.core.operators.sinks.batch

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkBatchSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractBatchSink

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 10:58 AM
  */
class CsvBatchSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractBatchSink(sparkAppContext, operatorContext, Map.empty) {
  override def customizeSourceConfig(readerConfig: SparkBatchSinkConfig): SparkBatchSinkConfig = readerConfig.copy(format = "csv")
}
