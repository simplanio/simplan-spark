package com.intuit.data.simplan.spark.core.operators.sources.stream

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.global.domain.QualifiedParam
import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.kafka.{FormatOptions, KafkaDataframeFormatter}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.types.StructType

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Sep-2021 at 10:39 AM
  */

class KafkaStreamingSourceConfig(
    val topic: String,
    val format: String = "JSON",
    val payloadSchema: Option[QualifiedParam] = None,
    val options: Map[String, String]
) extends OperatorConfig

class KafkaStreamingSource(
    sparkAppContext: SparkAppContext,
    operatorContext: OperatorContext
) extends SparkOperator[KafkaStreamingSourceConfig](sparkAppContext, operatorContext) {

  val resolvedOptions: Map[String, String] = operatorConfig.options ++ Map("includeHeaders" -> "true")

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val formatOptions = FormatOptions(operatorConfig.format, Option(operatorConfig.payloadSchema.get.resolveAs[StructType]))
    val loadedDataframe = sparkAppContext.spark.readStream
      .format("kafka")
      .options(resolvedOptions)
      .load()
      .pipe(each => KafkaDataframeFormatter.formatKafkaMessage(each, formatOptions))
    SparkOperatorResponse.continue(operatorContext.taskName, loadedDataframe)
  }
}
