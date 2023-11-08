/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intuit.data.simplan.spark.core.handlers.mapgroupwithstate

import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.SparkOperatorRequest
import com.intuit.data.simplan.spark.core.domain.streaming.AbstractMapGroupsWithStateHandler
import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.{Encoder, Encoders}

/** @author Abraham, Thomas - tabraham1
  *         Created on 05-Sep-2022 at 2:55 PM
  */

case class EntityEvents(vendorId: Long, companyId: Long, active: Boolean, pseudonymId: String, eventTimestamp: Long, entityChangeAction: String)
case class EntityActivenessExhaust(companyId: Long, active: Long, inactive: Long) extends Serializable
case class VendorState(active: Boolean, lastUpdatedTimestamp: Long)

class EntityActivenessTrackingHandler(sparkAppContext: SparkAppContext, parentOperatorRequest: SparkOperatorRequest, handlerConfigString: String) extends AbstractMapGroupsWithStateHandler(sparkAppContext, parentOperatorRequest, handlerConfigString) {
  override type KEY = Long
  override type STATE = Map[Long, VendorState]
  override type EVENTS = EntityEvents
  override type OUT = EntityActivenessExhaust
  override implicit val keyEncoder: Encoder[Long] = Encoders.scalaLong
  override implicit val stateEncoder: Encoder[Map[Long, VendorState]] = Encoders.kryo[Map[Long, VendorState]]
  override implicit val eventEncoder: Encoder[EntityEvents] = Encoders.product[EntityEvents]
  override implicit val outputEncoder: Encoder[EntityActivenessExhaust] = Encoders.product[EntityActivenessExhaust]

  override def grouping(events: EntityEvents): Long = events.companyId

  override def handle(companyId: Long, events: Iterator[EntityEvents], state: GroupState[Map[Long, VendorState]]): EntityActivenessExhaust = {
    if (events.isEmpty && state.hasTimedOut) {
      state.remove()
      EntityActivenessExhaust(0, 0, 0)
    } else {
      val currentCompanyState: Map[Long, VendorState] = state.getOption.getOrElse(Map.empty[Long, VendorState])
      val resolvedState: Map[Long, VendorState] = events.foldLeft(currentCompanyState) { (acc, curr) =>
        val timestampInState = acc.get(curr.vendorId).map(_.lastUpdatedTimestamp)
        if (timestampInState.isEmpty || timestampInState.get < curr.eventTimestamp) // Perform only if vendor doesnt exist in Map or timestampInState less than incoming event's timestamp
          if (curr.entityChangeAction.equalsIgnoreCase("DELETE")) acc - curr.vendorId
          else acc + (curr.vendorId -> VendorState(curr.active, curr.eventTimestamp))
        else acc
      }
      state.update(resolvedState)
      val activeList = resolvedState.filter(_._2.active).keys
      val inActiveList = resolvedState.filter(!_._2.active).keys
      EntityActivenessExhaust(companyId, activeList.size, inActiveList.size)

    }
  }
}
