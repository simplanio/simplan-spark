package com.intuit.data.simplan.spark.core.domain

import com.intuit.data.simplan.core.domain.operator.OperatorResponse
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 9:10 PM
  */
class SparkOperatorResponse(
    override val canContinue: Boolean,
    val dataframes: Map[String, DataFrame] = Map.empty,
    val deltaTable: Map[String, DeltaTable] = Map.empty,
    override val responseValues: Map[String, Serializable] = Map.empty,
    override val throwable: Option[Throwable] = None,
    override val message: String = ""
) extends OperatorResponse(canContinue, responseValues, throwable, message) {

  def copy(
      canContinue: Boolean = this.canContinue,
      dataframes: Map[String, DataFrame] = this.dataframes,
      deltaTable: Map[String, DeltaTable] = this.deltaTable,
      responseValues: Map[String, Serializable] = this.responseValues,
      throwable: Option[Throwable] = this.throwable,
      message: String = this.message) = new SparkOperatorResponse(canContinue, dataframes, deltaTable, responseValues, throwable, message)
}

object SparkOperatorResponse {
  def continue = new SparkOperatorResponse(canContinue = true)
  def continue(message: String) = new SparkOperatorResponse(canContinue = true, message = message)

  def dontContinue(message: String) = new SparkOperatorResponse(canContinue = false, message = message)
  def dontContinue(throwable: Throwable) = new SparkOperatorResponse(canContinue = false, throwable = Option(throwable))
  def dontContinue(message: String, throwable: Throwable) = new SparkOperatorResponse(canContinue = false, message = message, throwable = Option(throwable))

  def shouldContinue(canContinue: Boolean) = new SparkOperatorResponse(canContinue)

  def apply(dfReference: String, dataframe: DataFrame, canContinue: Boolean = true, responseValues: Map[String, Serializable] = Map.empty) =
    new SparkOperatorResponse(canContinue = true, Map(dfReference -> dataframe), responseValues = responseValues)
}
