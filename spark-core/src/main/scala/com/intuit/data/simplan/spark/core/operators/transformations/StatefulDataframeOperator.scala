package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.Row
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

case class StatefulDataframeOperatorConfig(source: String, stateTimeOutType: String, timeOut: Int, keyColumn: String, stateFunction: Option[String]) extends OperatorConfig

class StatefulDataframeOperator(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[StatefulDataframeOperatorConfig](sparkAppContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val frame = request.dataframes(operatorConfig.source)
    val timeOut = operatorConfig.timeOut
    val timeOutType: GroupStateTimeout = operatorConfig.stateTimeOutType match {
      case "event"      => GroupStateTimeout.EventTimeTimeout()
      case "processing" => GroupStateTimeout.ProcessingTimeTimeout()
      case "notimeout"  => GroupStateTimeout.NoTimeout()
    }

    import sparkAppContext.spark.implicits._
    val eventTimeExpirationFunc: (Long, Iterator[Row], GroupState[Map[Long, Seq[Long]]]) => Map[Long, Seq[Long]] = (key, values, state) => {
      if (values.isEmpty && state.hasTimedOut) {
        val oldStateOpt = state.getOption
        val oldState = state.getOption.getOrElse(Map.empty)
        state.remove()
        oldState
      } else {
        val currentWatermarkMs = state.getCurrentWatermarkMs()
        val expirationTime = currentWatermarkMs + operatorConfig.timeOut
        if (state.getOption.isEmpty) {
          state.setTimeoutTimestamp(expirationTime)
        }
        val prevState: Map[Long, Seq[Long]] = state.getOption.getOrElse(Map.empty)
        val newValues: Seq[Long] = prevState.getOrElse(key, Seq.empty) ++ values.map(row => (row.getAs[Long]("someNumber") + row.getAs[Long]("anotherNumber")))
        val newState = Map(key -> newValues)
        state.update(newState)
        state.setTimeoutTimestamp(expirationTime)
        newState
      }
    }

    val outputFrame = frame
      //.withWatermark("timestamp", "120 seconds")
      .groupByKey(row => row.getAs[Long](operatorConfig.keyColumn))
      .mapGroupsWithState(timeOutType)(eventTimeExpirationFunc).toDF()

    new SparkOperatorResponse(true, Map(operatorContext.taskName -> outputFrame))
  }

}
