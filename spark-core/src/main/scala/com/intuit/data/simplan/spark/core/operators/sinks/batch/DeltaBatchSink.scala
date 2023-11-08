package com.intuit.data.simplan.spark.core.operators.sinks.batch

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkBatchSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractBatchSink

/** @author Kiran Hiremath
  */
class DeltaBatchSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractBatchSink(sparkAppContext, operatorContext, Map.empty) {
  override def customizeSourceConfig(readerConfig: SparkBatchSinkConfig): SparkBatchSinkConfig = readerConfig.copy(format = "delta")
}
