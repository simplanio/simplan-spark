package com.intuit.data.simplan.spark.core.operators.sinks

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.util.OpsMetricsUtils
import com.intuit.data.simplan.global.exceptions.SimplanExecutionException
import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import com.intuit.data.simplan.logging.{Logging, MetricConstants}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkBatchSinkConfig
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse, SparkOperatorSettings}
import com.intuit.data.simplan.spark.core.events.CountDistinctByColumns
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.utils.DataframeUtils
import org.apache.spark.sql.{DataFrame, DataFrameWriter, Row}

import java.util
import scala.util.{Failure, Success, Try}

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 10:48 AM
  */
abstract class AbstractBatchSink(
    sparkAppContext: SparkAppContext,
    operatorContext: OperatorContext,
    defaultOptions: Map[String, String] = Map.empty
) extends SparkOperator[SparkBatchSinkConfig](
      sparkAppContext,
      operatorContext,
      SparkOperatorSettings(allowCaching = false, allowRepartition = false, allowMetrics = false)
    )
    with Logging {

  override implicit lazy val operatorConfig: SparkBatchSinkConfig = customizeSourceConfig(operatorContext.parseConfigAs[SparkBatchSinkConfig])

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    Try {
      val sourceFrame = request
        .dataframes(operatorConfig.source)
        .pipe(applyRepartitionIfNeeded) // Apply repartitioning if needed

      val withFinalTransformation = customiseFinalTransformation(sourceFrame)

      SparkOperatorResponse.continue(operatorContext.taskName, withFinalTransformation)
        .pipe(response => doCachingIfDefined(response))
        .pipe(response => computeMetricsIfDefined(response)).dataframes.head._2

      performColumnUniquenessCheck(withFinalTransformation)

      val dataframeWriter: DataFrameWriter[Row] = withFinalTransformation
        .write
        .format(operatorConfig.format)
        .mode(operatorConfig.resolvedSaveMode)
        .pipe(applyOptionsIfAvailable)
        .pipe(applyPartitionBy)
      val locationType = operatorConfig.locationType.getOrElse("path").toLowerCase
      logger.debug("Setting Location type as " + locationType)
      locationType match {
        case "path"  => dataframeWriter.save(operatorConfig.location)
        case "table" => dataframeWriter.saveAsTable(operatorConfig.location)
        case _       => throw new SimplanExecutionException(s"Invalid location type ${operatorConfig.locationType}. Allowed values are path or table")
      }
    } match {
      case Success(_) => SparkOperatorResponse.continue
      case Failure(exception) =>
        val message = s"Failed to write ${operatorConfig.format} file at ${operatorConfig.location}"
        throw new SimplanExecutionException(message, exception)
    }
  }

  def applyOptionsIfAvailable(writer: DataFrameWriter[Row]): DataFrameWriter[Row] = {
    val allOptions = defaultOptions ++ operatorConfig.resolvedOptions
    allOptions match {
      case options if options.nonEmpty => writer.options(allOptions)
      case _                           => writer
    }
  }

  def performColumnUniquenessCheck(dataframe: DataFrame): Unit = {
    val columns = operatorConfig.resolvedColumnUniquenessValidation
    if (columns.nonEmpty) {
      if (operatorOptions.cache.isEmpty) dataframe.cache()
      val rowCount = DataframeUtils.count(dataframe)
      val columnUniquenessRowCount = DataframeUtils.count(dataframe, columns)
      val aggs: util.Map[String, Object] = new util.HashMap[String, Object]()
      aggs.put("count", new java.lang.Long(rowCount))
      val distinct = new CountDistinctByColumns().setCount(columnUniquenessRowCount)
      columns.foreach(each => distinct.addColumn(each))
      aggs.put("countDistinctWithColumns", distinct)
      val opsEvent = OpsMetricsUtils.fromOperatorContext(
        operatorContext,
        s"Operator ${operatorContext.taskName}(${operatorContext.operatorType}) metrics",
        MetricConstants.Action.OPERATOR_METRICS,
        MetricConstants.Type.METRIC
      )
        .setEventData(aggs)
      sparkAppContext.opsMetricsEmitter.info(opsEvent)
      if (rowCount != columnUniquenessRowCount) {
        throw new SimplanExecutionException(
          s"Column uniqueness validation failed for columns ${columns.mkString(",")} in ${operatorContext.taskName} - ${operatorContext.operatorDefinition.operator}(${operatorContext.operatorType})")
      }
    }
  }

  private def applyPartitionBy(writer: DataFrameWriter[Row]): DataFrameWriter[Row] =
    operatorConfig.resolvedPartitionBy match {
      case partitionBy if partitionBy.nonEmpty => writer.partitionBy(partitionBy: _*)
      case _                                   => writer
    }

  private def applyRepartitionIfNeeded(dataframe: DataFrame): DataFrame = {
    DataframeUtils.repartition(sparkAppContext.spark, dataframe, operatorConfig.resolvedRepartition)._1
  }

  def customizeSourceConfig(readerConfig: SparkBatchSinkConfig): SparkBatchSinkConfig = readerConfig

  def customiseFinalTransformation(dataFrame: DataFrame): DataFrame = dataFrame
}
