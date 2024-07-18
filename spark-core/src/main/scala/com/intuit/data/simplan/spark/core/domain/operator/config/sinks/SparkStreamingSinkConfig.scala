package com.intuit.data.simplan.spark.core.domain.operator.config.sinks

import com.intuit.data.simplan.core.domain.operator.config.sinks.StreamingSinkConfig

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Nov-2021 at 3:02 PM
  */
case class SparkStreamingSinkConfig(
    trigger: StreamingTriggerConfig = StreamingTriggerConfig(),
    forEachBatch: Option[ForEachBatchDataframeWriterConfig],
    override val source: String,
    override val outputMode: String,
    override val format: String,
    override val options: Map[String, String] = Map.empty,
    override val awaitTermination: Boolean = true
) extends StreamingSinkConfig(source, outputMode, format, options, awaitTermination) {
  val resolvedTrigger = if (trigger == null) StreamingTriggerConfig() else trigger
  val resolvedOutputMode = if (outputMode == null) "append" else outputMode
}

case class ForEachBatchDataframeWriterConfig(handler: String, config: Option[AnyRef])
case class StreamingTriggerConfig(mode: String = "default", interval: Long = 0)
