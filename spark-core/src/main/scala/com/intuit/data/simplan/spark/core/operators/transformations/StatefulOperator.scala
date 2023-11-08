package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}

case class UserEvent(id: Int, data: String, isLast: Boolean)
case class UserSession(userEvents: Seq[UserEvent])

case class DeviceData(device: String, deviceType: String, signal: Double, time: String)

case class StatefulOperatorConfig(source: String, words: String) extends OperatorConfig

class StatefulOperator(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[StatefulOperatorConfig](sparkAppContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    implicit val userEventEncoder: Encoder[UserEvent] = Encoders.product[UserEvent]
    implicit val userSessionEncoder: Encoder[Option[UserSession]] =
      Encoders.kryo[Option[UserSession]]
    import sparkAppContext.spark.implicits._
    //val ds = request.dataframes[operatorConfig.source]
    val userEventsStream = request.dataframes(operatorConfig.source).as[UserEvent]
    val finishedUserEventsStream = userEventsStream.groupByKey(_.id).mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateSessionEvents)
      .flatMap(userSession => userSession)
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> finishedUserEventsStream.toDF()))

  }

  def updateSessionEvents(id: Int, userEvents: Iterator[UserEvent], state: GroupState[UserSession]): Option[UserSession] = {

    if (state.hasTimedOut) {
      state.remove()
      state.getOption
    } else {
      val currentState = state.getOption
      val updatedUserSession = currentState.fold(UserSession(userEvents.toSeq))(currentUserSession => UserSession(currentUserSession.userEvents ++ userEvents.toSeq))
      state.update(updatedUserSession)
      if (updatedUserSession.userEvents.exists(_.isLast)) {

        val userSession = state.getOption
        state.remove()
        userSession
      } else {
        state.setTimeoutDuration("1 minute")
        None
      }
    }
  }
}
