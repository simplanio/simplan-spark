package com.intuit.data.simplan.spark.core.operators.sinks.stream

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkStreamingSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractStreamingSink

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Sep-2021 at 3:55 PM
  */
class ParquetStreamingSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractStreamingSink(sparkAppContext, operatorContext) {
  override def customizeConfig(readerConfig: SparkStreamingSinkConfig): SparkStreamingSinkConfig = readerConfig.copy(format = "parquet")

}
