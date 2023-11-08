package com.intuit.data.simplan.spark.core.domain.streaming

import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.SparkOperatorRequest
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{DataFrame, Dataset, Encoder}

/** @author Abraham, Thomas - tabraham1
  *         Created on 29-Apr-2022 at 12:57 PM
  */
abstract class AbstractMapGroupsWithStateHandler(appContext: SparkAppContext, parentOperatorRequest: SparkOperatorRequest, handlerConfigString: String) extends Serializable {
  type KEY
  type EVENTS
  type STATE
  type OUT

  implicit val keyEncoder: Encoder[KEY]
  implicit val eventEncoder: Encoder[EVENTS]
  implicit val stateEncoder: Encoder[STATE]
  implicit val outputEncoder: Encoder[OUT]

  def grouping(events: EVENTS): KEY

  def handle(key: KEY, events: Iterator[EVENTS], state: GroupState[STATE]): OUT

  def preTransformation(dataframe: DataFrame): Dataset[EVENTS] = dataframe.as[EVENTS]

  def postTransformation(dataset: Dataset[OUT]): DataFrame = dataset.toDF
}
