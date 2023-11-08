package com.intuit.data.simplan.spark.core.operators.sources.batch

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sources.SparkBatchSourceConfig
import com.intuit.data.simplan.spark.core.operators.sources.AbstractBatchSource

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Sep-2021 at 10:39 AM
  */
class BatchSource(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractBatchSource(sparkAppContext, operatorContext) {
  override def customizeReaderConfig(sourceConfig: SparkBatchSourceConfig): SparkBatchSourceConfig = sourceConfig.format match {
    case null => sourceConfig.copy(format = "parquet")
    case _ => sourceConfig
  }
}
