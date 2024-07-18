package com.intuit.data.simplan.spark.core.operators.domainevents

import com.intuit.data.simplan.core.domain.TableType
import com.intuit.data.simplan.core.domain.TableType._
import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.Constants._
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.utils.DebugUtils
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/** @author Abraham, Thomas - tabraham1
  *         Created on 29-Nov-2021 at 9:54 AM
  */
case class DomainEventParserConfig(source: String, extract: Boolean = false, primaryKeys: Seq[String] = DefaultPrimaryKeys, tableType: TableType = TableType.TEMP) extends OperatorConfig

class DomainEventParser(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[DomainEventParserConfig](appContext, operatorContext) with Logging {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val sourceDF = request.dataframes(operatorConfig.source)

    val deduplicatedDf = dedupeByPrimaryKey(sourceDF, operatorConfig.primaryKeys)
    DebugUtils.printSchema("With deduplicated PK", deduplicatedDf)

    val parsedDomainEventDF = deduplicatedDf.select(col(ColumnHeaders), col(ColumnHeadersRaw), col(ColumnValue))
    if (operatorConfig.tableType == TEMP)
      parsedDomainEventDF.createOrReplaceTempView(operatorContext.taskName)

    SparkOperatorResponse(operatorContext.taskName, parsedDomainEventDF)
  }

  def dedupeByPrimaryKey(dataframe: DataFrame, primaryKeyColNames: Seq[String]) = {
    val AliasDedupCandidate = "dedupCandidate"
    val AliasDedupWinner = "dedupWinner"
    val AliasPrimaryKey = "primaryKey"
    import appContext.spark.implicits._

    dataframe
      .withWatermark("timestamp", "1 minutes")
      .select(
        struct(primaryKeyColNames.map(colName => col(s"$ColumnValue.$colName")): _*).as(AliasPrimaryKey),
        struct(
          col(s"$ColumnHeaders.$ColumnHeaderEntityVersion"), // The event with the largest value should prevail
          col(ColumnHeaders),
          col(ColumnHeadersRaw),
          col(ColumnValue)
        ).as(AliasDedupCandidate),
        col("timestamp")
      ).groupBy(window($"timestamp", "1 minutes", "1 minutes"), col(AliasPrimaryKey))
      .agg(max(AliasDedupCandidate).as(AliasDedupWinner))
      .select(
        col(s"$AliasDedupWinner.$ColumnHeaders"),
        col(s"$AliasDedupWinner.$ColumnHeadersRaw"),
        col(s"$AliasDedupWinner.$ColumnValue")
      )
  }
}
