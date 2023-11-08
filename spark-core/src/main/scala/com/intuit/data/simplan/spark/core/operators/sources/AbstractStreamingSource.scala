package com.intuit.data.simplan.spark.core.operators.sources

import com.intuit.data.simplan.core.domain.StreamingParseMode._
import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.sources.{StreamSourceConfig, WaterMarkConfig}
import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sources.SparkStreamSourceConfig
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.utils.{DataframeUtils, SparkUDFs}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json, struct}
import org.apache.spark.sql.streaming.DataStreamReader
import org.apache.spark.sql.types.{StringType, StructType}

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Sep-2021 at 1:01 PM
  */

abstract class AbstractStreamingSource(sparkAppContext: SparkAppContext, operatorContext: OperatorContext, format: String, defaultOptions: Map[String, String] = Map.empty) extends SparkOperator[SparkStreamSourceConfig](sparkAppContext, operatorContext) {

  val PAYLOAD_COLUMN = "value"
  val HEADER_COLUMN = "headers"

  override lazy implicit val operatorConfig = customizeConfig(operatorContext.parseConfigAs[SparkStreamSourceConfig])

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val streamingDataframe = readInitialStreamRead
      .pipe(applyPreParseTransformation)
      .pipe(parseHeader)
      .pipe(parsePayload)
      .pipe(applyPostParseTransformation)
      .pipe(applyParseMode)
      .pipe(applyWatermark)
    DataframeUtils.createOrReplaceTempView(streamingDataframe, operatorConfig.tableType, operatorContext.taskName)
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> streamingDataframe))
  }

  def readInitialStreamRead(implicit config: StreamSourceConfig): DataFrame = {
    val allOptions = defaultOptions ++ config.options
    val reader: DataStreamReader = sparkAppContext.spark.readStream.format(format)
    if (allOptions.nonEmpty)
      reader.options(allOptions).load()
    else reader.load()
  }

  def parseHeader(dataframe: DataFrame)(implicit config: StreamSourceConfig): DataFrame = {
    //Process only of headerSchema is defined, if not return incoming dataframe
    if (config.headerSchema.isEmpty) return dataframe

    val headerSchema: StructType = config.headerSchema.get.resolveAs[StructType]
    val dfWithKey = dataframe.withColumn(config.HEADER_FIELD, col("key").cast(StringType))
    val columns = headerSchema.fields.map(each => col("headerMap").getItem(each.name).cast(each.dataType).as(each.name))
    dfWithKey
      .withColumnRenamed("headers", "headersRaw")
      .withColumn("headerMap", SparkUDFs.ArrayToMapUdf(col("headersRaw")))
      .withColumn(HEADER_COLUMN, struct(columns: _*))
      .drop("headerMap")
  }

  def parsePayload(dataframe: DataFrame)(implicit config: StreamSourceConfig): DataFrame = {
    //Process only of payloadSchema is defined, if not return incoming dataframe
    if (config.payloadSchema.isEmpty) return dataframe

    val payloadSchema: StructType = config.payloadSchema.get.resolveAs[StructType]
    config.format.toUpperCase match {
      case "JSON" => dataframe.withColumn(PAYLOAD_COLUMN, from_json(col(PAYLOAD_COLUMN).cast(StringType), payloadSchema)) //.drop("value")
      case _      => dataframe
    }
  }

  def applyParseMode(dataframe: DataFrame)(implicit config: StreamSourceConfig): DataFrame = {
    config.RESOLVED_PARSE_MODE match {
      case PAYLOAD_ONLY => dataframe.select(s"$PAYLOAD_COLUMN.*")
      case HEADER_ONLY  => dataframe.select(s"$HEADER_COLUMN.*")
      case ALL_PARSED   => dataframe.drop("headersRaw")
      case _            => dataframe
    }
  }

  def applyWatermark(dataframe: DataFrame)(implicit config: StreamSourceConfig): DataFrame = {
    config.watermark match {
      case Some(waterMarkConfig: WaterMarkConfig) => dataframe.withWatermark(waterMarkConfig.eventTime, waterMarkConfig.delayThreshold)
      case None                                   => dataframe
    }
  }
  def customizeConfig(readerConfig: SparkStreamSourceConfig): SparkStreamSourceConfig = readerConfig

  def applyPreParseTransformation(dataFrame: DataFrame): DataFrame = dataFrame

  def applyPostParseTransformation(dataFrame: DataFrame): DataFrame = dataFrame

}
