package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.global.utils.SimplanImplicits.ToJsonImplicits
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.streaming.AbstractMapGroupsWithStateHandler
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.commons.lang.StringUtils
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.streaming.GroupStateTimeout

/** @author Abraham, Thomas - tabraham1
  *         Created on 29-Apr-2022 at 1:13 PM
  */
case class MapGroupWithStateOperatorConfig(source: String, timeoutType: String, timeoutDuration: Long, handler: String, handlerConfig: Option[AnyRef]) extends OperatorConfig

class MapGroupWithStateOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[MapGroupWithStateOperatorConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {

    val handler = Class.forName(operatorConfig.handler).getConstructor(classOf[SparkAppContext], classOf[SparkOperatorRequest], classOf[String]).newInstance(appContext, request, operatorConfig.handlerConfig.getOrElse("{}").toJson).asInstanceOf[AbstractMapGroupsWithStateHandler]

    implicit val keyEncoder = handler.keyEncoder
    implicit val eventEncoder = handler.eventEncoder
    implicit val stateEncoder = handler.stateEncoder
    implicit val outputEncoder = handler.outputEncoder
    val timeOutType: GroupStateTimeout = Option(operatorConfig.timeoutType).getOrElse(StringUtils.EMPTY).toUpperCase match {
      case "EVENT"      => GroupStateTimeout.EventTimeTimeout()
      case "PROCESSING" => GroupStateTimeout.ProcessingTimeTimeout()
      case _            => GroupStateTimeout.NoTimeout()
    }
    val castedToDataSet = handler.preTransformation(request.dataframes(operatorConfig.source))

    val finishedUserEventsStream: Dataset[handler.OUT] = castedToDataSet
      .groupByKey(handler.grouping)
      .mapGroupsWithState(timeOutType)(handler.handle)

    val postTransformed = handler.postTransformation(finishedUserEventsStream)
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> postTransformed))
  }
}
