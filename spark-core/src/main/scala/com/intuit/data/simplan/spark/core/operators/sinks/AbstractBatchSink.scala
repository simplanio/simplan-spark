package com.intuit.data.simplan.spark.core.operators.sinks

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.global.exceptions.SimplanExecutionException
import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkBatchSinkConfig
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse, SparkOperatorSettings}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.utils.DataframeUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row, SaveMode}

import scala.util.{Failure, Success, Try}

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 10:48 AM
  */
abstract class AbstractBatchSink(
    sparkAppContext: SparkAppContext,
    operatorContext: OperatorContext,
    defaultOptions: Map[String, String] = Map.empty,
    saveMode: SaveMode = SaveMode.Overwrite
) extends SparkOperator[SparkBatchSinkConfig](
      sparkAppContext,
      operatorContext,
      SparkOperatorSettings(allowCaching = false, allowRepartition = false)
    )
    with Logging {

  override implicit lazy val operatorConfig: SparkBatchSinkConfig = customizeSourceConfig(operatorContext.parseConfigAs[SparkBatchSinkConfig])

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    Try {
      val sourceFrame = request
        .dataframes(operatorConfig.source)
        .pipe(applyRepartitionIfNeeded) // Apply repartitioning if needed

      val withFinalTransformation = customiseFinalTransformation(sourceFrame)
      val dataframeWriter: DataFrameWriter[Row] = withFinalTransformation
        .write
        .format(operatorConfig.format)
        .mode(saveMode)
        .pipe(applyOptionsIfAvailable)
        .pipe(applyPartitionBy)

      dataframeWriter.save(operatorConfig.location)
    } match {
      case Success(_) => SparkOperatorResponse.continue
      case Failure(exception) =>
        val message = s"Failed to write ${operatorConfig.format} file at ${operatorConfig.location}"
        throw new SimplanExecutionException(message, exception)
    }

  }

  private def applyOptionsIfAvailable(writer: DataFrameWriter[Row]): DataFrameWriter[Row] = {
    val allOptions = defaultOptions ++ operatorConfig.resolvedOptions
    allOptions match {
      case options if options.nonEmpty => writer.options(allOptions)
      case _                           => writer
    }
  }

  private def applyPartitionBy(writer: DataFrameWriter[Row]): DataFrameWriter[Row] = operatorConfig.resolvedPartitionBy match {
    case partitionBy if partitionBy.nonEmpty => writer.partitionBy(partitionBy: _*)
    case _                                   => writer
  }

  private def applyRepartitionIfNeeded(dataframe: DataFrame): DataFrame = {
    DataframeUtils.repartitionDataframe(sparkAppContext.spark, dataframe, operatorConfig.resolvedRepartition)._1
  }

  def customizeSourceConfig(readerConfig: SparkBatchSinkConfig): SparkBatchSinkConfig = readerConfig

  def customiseFinalTransformation(dataFrame: DataFrame): DataFrame = dataFrame
}
