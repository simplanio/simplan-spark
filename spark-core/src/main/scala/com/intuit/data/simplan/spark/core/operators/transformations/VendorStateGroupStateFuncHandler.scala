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

package com.intuit.data.simplan.spark.core.operators.transformations

/*class VendorStateGroupStateFuncHandler extends AbstractSparkGroupStateFuncHandler {

  case class StatefulOperatorConfig[K:Encoder, V:Encoder, S: Encoder,U: Encoder](source: String, timeoutType: String, func:(K, Iterator[V], GroupState[S])=>U)
  case class VendorState(status:String, lastUpdatedTimestamp: Long)
  case class CompanyVendorActiveList(companyId: Long, activeCount: Long, inActiveCount:Long )
  case class EventStream(vendorId:Long, companyId:Long, active:Boolean, pseudonymId:Long, eventTimestamp:String, entityChangeAction:String, idempotenceKey:Long)

  implicit val vendorStateEncoder: Encoder[VendorState] = Encoders.product[VendorState]
  implicit val companyVendorActiveList: Encoder[Option[CompanyVendorActiveList]] = Encoders.kryo[CompanyVendorActiveList]

  def handle[K:Encoder, V:Encoder, S: Encoder,U: Encoder](K, Iterator[V], GroupState[S]):U ={
  }


  def updateCompanyVendorListState(companyId:Long, events: Iterator[EventStream], state:GroupState[Map[Long,Map[Long,VendorState]]]):Option[CompanyVendorActiveList] = {
    val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
    if (values.isEmpty && state.hasTimedOut) {
      val oldStateOpt = state.getOption
      val oldState = state.getOption.getOrElse(Map.empty)
      state.remove()
      val currTime = sdf.format(new Date())
      None
    } else {
      //val currentWatermarkMs = state.getCurrentWatermarkMs()
      //val expirationTime = currentWatermarkMs + config.timeOut
      val expirationTime = config.timeOut
      if (state.getOption.isEmpty && timeOutType != GroupStateTimeout.NoTimeout()) {
        state.setTimeoutTimestamp(expirationTime)
      }
      val prevState: Map[Long,Map[Long,VendorState]] = state.getOption.getOrElse(Map.empty)
      val prevElement: Option[Map[Long,VendorState]] = prevState.get(key)
      var activeCnt:Long = 0
      var inActiveCnt:Long = 0
      val latestState:Map[Long,(Boolean, VendorState)] = if (prevState.isEmpty || !prevElement.isDefined){
        val currState:Map[Long,VendorState]  = events.map(event =>{
          val currTime:Date = sdf.parse(event.eventTimestamp)
          if (event.active){
            activeCnt = activeCnt + 1
          }else inActiveCnt = inActiveCnt + 1
          (event.vendorId -> VendorState(event.active, currTime.getTime))
        }).toMap
        currState
      }else{
        val prevVendorMap = prevElement.get
        val currState:Map[Long,VendorState] = events.map(event =>{
          val currTime:Long = sdf.parse(event.eventTimestamp).getTime
          val vendorMap:Option[VendorState] = prevVendorMap.get(currVendorId)
          val prevActiveStatus =  vendorMap.get.status
          val prevTime:Long = vendorMap.get.lastUpdatedTimestamp
          if (prevActiveStatus == event.active)  {
            // do nothing no change
            None
          }else{
            if (currTime < prevTime){
              // do nothing no change , its already updated
              //(prevActiveStatus , prevTime)
              None
            }else{
              event.active match {
                // inactive to active
                case true => {
                  activeCnt = activeCnt + 1
                  inActiveCnt = inActiveCnt - 1
                }
                // active to inactive
                case false => {
                  activeCnt = activeCnt + 1
                  inActiveCnt = inActiveCnt - 1
                }
              }
              Some((currVendorId -> VendorState(currActiveStatus, currTime)))
            }
          }
        }).filter(_.isDefined).map(_.get).toMap
        state
      }

      state.update(Map(key-> latest))
      if ( timeOutType != GroupStateTimeout.NoTimeout()){
        state.setTimeoutTimestamp(expirationTime)
      }
      Map("active"->activeCnt , "inActive" -> inActiveCnt, "companyId" -> key)
    }

}*/
