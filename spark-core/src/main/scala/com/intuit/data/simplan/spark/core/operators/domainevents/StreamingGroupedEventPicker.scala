package com.intuit.data.simplan.spark.core.operators.domainevents

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.Constants._
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.functions._

/** @author Abraham, Thomas - tabraham1
  *         Created on 01-Dec-2021 at 12:27 PM
  */
case class StreamingGroupedEventPickerConfig(
    source: String,
    extract: Boolean = false,
    eventTimeField: String = "timestamp",
    delayThreshold: String = "1 minutes",
    windowTimeColumn: String = "timestamp",
    windowDuration: String = "1 minutes",
    windowSlideDuration: String = "1 minutes",
    groupedByFields: Seq[String] = Seq("primaryKey"),
    sequenceField: String = s"$ColumnHeaders.$ColumnHeaderEntityVersion",
    picker: String = "LARGEST")
    extends OperatorConfig

class StreamingGroupedEventPicker(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[StreamingGroupedEventPickerConfig](appContext, operatorContext) {
  val CANDIDATE = "CANDIDATE"
  val PICKED = "PICKED"
  val GROUPED_FIELDS = "GROUPED"

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val dataframe = request.dataframes(operatorConfig.source)

    val aggregationExpr = operatorConfig.picker.toUpperCase match {
      case "LARGEST" => max(CANDIDATE)
      case "LOWEST"  => min(CANDIDATE)
      case "AVERAGE" => avg(CANDIDATE)
      case "MEAN"    => mean(CANDIDATE)
    }

    val pickedEvents = dataframe
      .withWatermark(operatorConfig.eventTimeField, operatorConfig.delayThreshold)
      .select(
        struct(operatorConfig.groupedByFields.map(colName => col(colName)): _*).as(GROUPED_FIELDS),
        struct("*").as(CANDIDATE)
      ).groupBy(
        //window(col(CANDIDATE + "." + operatorConfig.windowTimeColumn), operatorConfig.windowDuration, operatorConfig.windowSlideDuration),
        col(GROUPED_FIELDS)
      )
      .agg(aggregationExpr.as(PICKED))
      .select(s"$PICKED.*")

    SparkOperatorResponse(operatorContext.taskName, pickedEvents)
  }
}
