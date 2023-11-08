package com.intuit.data.simplan.spark.core.handlers

import com.intuit.data.simplan.global.utils.SimplanImplicits.FromJsonImplicits
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.SparkOperatorRequest
import com.intuit.data.simplan.spark.core.domain.streaming.AbstractMapGroupsWithStateHandler
import com.intuit.data.simplan.spark.core.operators.transformations.{UserEvent, UserSession}
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Encoders}

/** @author Abraham, Thomas - tabraham1
  *         Created on 29-Apr-2022 at 1:27 PM
  */

case class UserSessionMapGroupWithStateHandlerConfig(timeoutDuration: String, groupBy: String)

class UserSessionMapGroupWithState(sparkAppContext: SparkAppContext, parentOperatorRequest: SparkOperatorRequest, handlerConfigString: String) extends AbstractMapGroupsWithStateHandler(sparkAppContext, parentOperatorRequest, handlerConfigString) {

  override type KEY = Int
  override type STATE = UserSession
  override type EVENTS = UserEvent
  override type OUT = Option[UserSession]
  val handlerConfig = handlerConfigString.fromJson[UserSessionMapGroupWithStateHandlerConfig]

  override def handle(key: Int, events: Iterator[UserEvent], state: GroupState[UserSession]): Option[UserSession] = {
    if (state.hasTimedOut) {
      state.remove()
      state.getOption
    } else {
      val currentState = state.getOption
      val seq = events.toSeq
      println("Printing : " + seq)
      val updatedUserSession = currentState.fold(UserSession(seq))(currentUserSession => UserSession(currentUserSession.userEvents ++ seq))
      state.update(updatedUserSession)
      if (updatedUserSession.userEvents.exists(_.isLast)) {
        val userSession = state.getOption
        state.remove()
        userSession
      } else {
        state.setTimeoutDuration(handlerConfig.timeoutDuration)
        None
      }
    }
  }

  override def grouping(events: UserEvent): Int = {
    events.id
  }

  override def postTransformation(dataset: Dataset[Option[UserSession]]): DataFrame = dataset.flatMap(x => x).toDF

  override implicit val keyEncoder: Encoder[Int] = Encoders.scalaInt
  override implicit val eventEncoder: Encoder[UserEvent] = Encoders.product[UserEvent]
  override implicit val stateEncoder: Encoder[UserSession] = Encoders.product[UserSession]
  override implicit val outputEncoder: Encoder[Option[UserSession]] = Encoders.kryo[Option[UserSession]]
}
