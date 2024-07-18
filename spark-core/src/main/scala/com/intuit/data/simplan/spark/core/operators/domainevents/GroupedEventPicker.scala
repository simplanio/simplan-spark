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
case class GroupedEventPickerConfig(source: String, extract: Boolean = false, groupedByFields: Seq[String] = DefaultPrimaryKeys, sequenceField: String = s"$ColumnHeaders.$ColumnHeaderEntityVersion", picker: String = "LARGEST") extends OperatorConfig

class GroupedEventPicker(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[GroupedEventPickerConfig](appContext, operatorContext) {
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

    val pickedEvents = dataframe.select(
      struct(operatorConfig.groupedByFields.map(colName => col(colName)): _*).as(GROUPED_FIELDS),
      struct("*").as(CANDIDATE)
    ).groupBy(GROUPED_FIELDS)
      .agg(aggregationExpr.as(PICKED))
      .select(s"$CANDIDATE.*")

    SparkOperatorResponse(operatorContext.taskName, pickedEvents)
  }
}
