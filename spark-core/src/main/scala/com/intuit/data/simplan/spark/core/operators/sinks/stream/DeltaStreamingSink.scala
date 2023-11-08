package com.intuit.data.simplan.spark.core.operators.sinks.stream

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkStreamingSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractStreamingSink

/** @author Kiran Hiremath
  */

class DeltaStreamingSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractStreamingSink(sparkAppContext, operatorContext) {
  override def customizeConfig(readerConfig: SparkStreamingSinkConfig): SparkStreamingSinkConfig = readerConfig.copy(format = "delta")

}
