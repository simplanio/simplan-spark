package com.intuit.data.simplan.spark.core.operators.sources

import com.intuit.data.simplan.core.domain.TableType
import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.util.FileOperationsUtils
import com.intuit.data.simplan.global.domain.QualifiedParam
import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sources.SparkBatchSourceConfig
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Sep-2021 at 9:42 AM
  */
abstract class AbstractBatchSource(sparkAppContext: SparkAppContext, operatorContext: OperatorContext, defaultOptions: Map[String, String] = Map.empty) extends SparkOperator[SparkBatchSourceConfig](sparkAppContext, operatorContext) with Logging {

  override implicit lazy val operatorConfig: SparkBatchSourceConfig = customizeReaderConfig(operatorContext.parseConfigAs[SparkBatchSourceConfig])

  val path = FileOperationsUtils.getSortedDirectory(sparkAppContext.fileUtils, operatorConfig.location, operatorConfig.directorySortPattern)

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val frame: DataFrame = loadBatchData
      .pipe(applyFilter)
      .pipe(applyProjection)
    val resolvedTableName = operatorConfig.table.getOrElse(operatorContext.taskName)
    if (operatorConfig.tableType == TableType.TEMP)
      frame.createOrReplaceTempView(resolvedTableName)
    else if (operatorConfig.tableType == TableType.MANAGED)
      frame.createOrReplaceTempView(resolvedTableName)
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> frame))
  }
  override def getJobDescription: String = super.getJobDescription + s"\nPath : $path"

  def applyPreTransforamtion(dataFrame: DataFrame): DataFrame = dataFrame

  def loadBatchData: DataFrame = {
    val allOptions = defaultOptions ++ operatorConfig.resolvedOptions
    val schema = getSchemaFromJson(operatorConfig.schema)
    logger.info(s"Resolved Path to load for ${operatorContext.taskName} : $path")
    val readWithSchema = if (schema.isDefined) sparkAppContext.spark.read.schema(schema.get) else sparkAppContext.spark.read
    allOptions match {
      case options if options.nonEmpty => readWithSchema.options(options).format(operatorConfig.format).load(path)
      case _                           => readWithSchema.format(operatorConfig.format).load(path)
    }
  }

  def applyProjection(frame: DataFrame): DataFrame = if (operatorConfig.resolvedProjections.nonEmpty) frame.selectExpr(operatorConfig.resolvedProjections: _*) else frame
  def applyFilter(frame: DataFrame): DataFrame = if (operatorConfig.filter.isDefined) frame.where(operatorConfig.filter.get) else frame

  def customizeReaderConfig(sourceConfig: SparkBatchSourceConfig): SparkBatchSourceConfig = sourceConfig

  def getSchemaFromJson(path: Option[QualifiedParam]): Option[StructType] = path.map(_.resolveAs[StructType])

}
