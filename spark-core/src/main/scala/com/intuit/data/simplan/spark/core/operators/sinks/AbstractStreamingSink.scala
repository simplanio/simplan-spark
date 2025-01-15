package com.intuit.data.simplan.spark.core.operators.sinks

import com.intuit.data.simplan.core.domain.SinkLocationType
import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.global.utils.SimplanImplicits.{Pipe, ToJsonImplicits}
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkStreamingSinkConfig
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse, TriggerModes}
import com.intuit.data.simplan.spark.core.handlers.foreachbatch.ForEachBatchHandler
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.streaming.{DataStreamWriter, Trigger}
import org.apache.spark.sql.{DataFrame, Row}

/** @author Abraham, Thomas - tabraham1
 *          Created on 17-Sep-2021 at 3:48 PM
 */

abstract class AbstractStreamingSink(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[SparkStreamingSinkConfig](appContext, operatorContext) with Logging {

  override implicit lazy val operatorConfig: SparkStreamingSinkConfig = customizeConfig(operatorContext.parseConfigAs[SparkStreamingSinkConfig])

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val frame = request.dataframes(operatorConfig.source)
    val transformedDF = customiseSinkTransformation(frame)
    implicit val operatorRequest: SparkOperatorRequest = request
    val dataStreamWriter: DataStreamWriter[Row] = streamSinkInit(transformedDF, operatorContext.taskName)
      .pipe(applyForEachBatch)
    val streamingQuery = if (operatorConfig.locationType.isDefined && operatorConfig.locationType.get == SinkLocationType.PATH)
      dataStreamWriter.option("path", operatorConfig.location).start()
    else dataStreamWriter.toTable(operatorConfig.location)
    if (operatorConfig.awaitTermination) streamingQuery.awaitTermination()

    SparkOperatorResponse.continue
  }

  def streamSinkInit(dataframe: DataFrame, name: String): DataStreamWriter[Row] = {
    val trigger = TriggerModes(operatorConfig.resolvedTrigger.mode, operatorConfig.resolvedTrigger.interval)
    logger.debug(s"Trigger for ${operatorContext.taskName}: $trigger")
    dataframe.writeStream
      .queryName(name)
      .format(operatorConfig.format)
      .outputMode(operatorConfig.resolvedOutputMode)
      .trigger(trigger)
      .options(operatorConfig.options)
  }

  def applyForEachBatch(writer: DataStreamWriter[Row])(implicit operatorConfig: SparkStreamingSinkConfig, operatorRequest: SparkOperatorRequest): DataStreamWriter[Row] = {
    if (operatorConfig.forEachBatch != null && operatorConfig.forEachBatch.isDefined) {
      val handler: ForEachBatchHandler = Class.forName(operatorConfig.forEachBatch.get.handler).getConstructor(classOf[SparkAppContext]).newInstance(appContext).asInstanceOf[ForEachBatchHandler]
      val forEachBatchFunction = handler.handle(operatorRequest, operatorConfig.forEachBatch.get.config.getOrElse("{}").toJson)
      writer.foreachBatch(forEachBatchFunction)
    } else writer
  }

  def customizeConfig(readerConfig: SparkStreamingSinkConfig): SparkStreamingSinkConfig = readerConfig

  def customiseSinkTransformation(dataFrame: DataFrame): DataFrame = dataFrame

}
