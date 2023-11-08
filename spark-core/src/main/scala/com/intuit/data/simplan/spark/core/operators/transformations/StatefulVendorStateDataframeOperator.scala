//package com.intuit.data.simplan.spark.core.operators.transformations
//
//import com.intuit.data.simplan.core.domain.operator.OperatorContext
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
//import com.intuit.data.simplan.spark.core.operators.SparkOperator
//import org.apache.spark.sql.Row
//import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
//
//import java.text.SimpleDateFormat
//import java.util.Date
//
//class StatefulVendorStateDataframeOperator(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[StatefulDataframeOperatorConfig](sparkAppContext, operatorContext) {
//
//  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
//    val frame = request.dataframes(operatorConfig.source)
//    val timeOut = operatorConfig.timeOut
//    val timeOutType: GroupStateTimeout = operatorConfig.stateTimeOutType match {
//      case "event"      => GroupStateTimeout.EventTimeTimeout()
//      case "processing" => GroupStateTimeout.ProcessingTimeTimeout()
//      case "notimeout"  => GroupStateTimeout.NoTimeout()
//    }
//    val eventTimeExpirationFunc: (Long, Iterator[Row], GroupState[Map[Long, Map[Long, (Boolean, Long)]]]) => Map[String, Long] = (key, values, state) => {
//      val sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
//      if (values.isEmpty && state.hasTimedOut) {
//        val oldStateOpt = state.getOption
//        val oldState = state.getOption.getOrElse(Map.empty)
//        state.remove()
//        val currTime = sdf.format(new Date())
//        Map.empty[String, Long]
//      } else {
//        val expirationTime = operatorConfig.timeOut
//        if (state.getOption.isEmpty && timeOutType != GroupStateTimeout.NoTimeout()) {
//          state.setTimeoutTimestamp(expirationTime)
//        }
//        val prevState: Map[Long, Map[Long, (Boolean, Long)]] = state.getOption.getOrElse(Map.empty)
//        val prevElement: Option[Map[Long, (Boolean, Long)]] = prevState.get(key)
//        var activeCnt: Long = 0
//        var inActiveCnt: Long = 0
//        val newState: Map[Long, (Boolean, Long)] =
//          if (prevState.isEmpty || !prevElement.isDefined) {
//            val state: Map[Long, (Boolean, Long)] = values.map(row => {
//              val currVendorId: Long = row.getAs[Long]("vendorId")
//              val currCompanyId = row.getAs[Long]("companyId")
//              val entityChangeAction = row.getAs[String]("entityChangeAction")
//              val currActiveStatus: Boolean = row.getAs[Boolean]("active")
//              val currTimeStr: String = row.getAs[String]("eventTimestamp")
//              val currTime: Date = sdf.parse(currTimeStr)
//              if (entityChangeAction.equalsIgnoreCase("DELETE")) {
//                inActiveCnt = inActiveCnt + 1
//                activeCnt = activeCnt - 1
//              } else {
//                if (currActiveStatus) {
//                  activeCnt = activeCnt + 1
//                } else inActiveCnt = inActiveCnt + 1
//              }
//              (currVendorId -> (currActiveStatus, currTime.getTime))
//            }).toMap
//            state
//          } else {
//            val prevVendorMap = prevElement.get
//            val state: Map[Long, (Boolean, Long)] = values.map(row => {
//              val currCompanyId = row.getAs[Long]("companyId")
//              val currVendorId: Long = row.getAs[Long]("vendorId")
//              val entityChangeAction = row.getAs[String]("entityChangeAction")
//              val currActiveStatus = row.getAs[Boolean]("active")
//              val currEventTimeStr = row.getAs[String]("eventTimestamp")
//              val currTime: Long = sdf.parse(currEventTimeStr).getTime
//              val vendorMap: Option[(Boolean, Long)] = prevVendorMap.get(currVendorId)
//              if (vendorMap.isDefined) {
//                val prevActiveStatus = vendorMap.get._1
//                val prevTime: Long = vendorMap.get._2
//                if (prevActiveStatus == currActiveStatus) {
//                  // do nothing no change
//                  None
//                } else {
//                  if (currTime < prevTime) {
//                    // do nothing no change , its already updated
//                    None
//                  } else {
//                    if (entityChangeAction.equalsIgnoreCase("DELETE")) {
//                      inActiveCnt = inActiveCnt + 1
//                      activeCnt = activeCnt - 1
//                    } else {
//                      currActiveStatus match {
//                        // inactive to active
//                        case true => {
//                          activeCnt = activeCnt + 1
//                          inActiveCnt = inActiveCnt - 1
//                        }
//                        // active to inactive
//                        case false => {
//                          activeCnt = activeCnt + 1
//                          inActiveCnt = inActiveCnt - 1
//                        }
//                      }
//                    }
//                    Some((currVendorId -> (currActiveStatus, currTime)))
//                  }
//                }
//              } else {
//                if (entityChangeAction.equalsIgnoreCase("DELETE")) {
//                  inActiveCnt = inActiveCnt + 1
//                  activeCnt = activeCnt - 1
//                } else {
//                  if (currActiveStatus) {
//                    activeCnt = activeCnt + 1
//                  } else inActiveCnt = inActiveCnt + 1
//                }
//                Some((currVendorId -> (currActiveStatus, currTime)))
//              }
//
//            }).filter(_.isDefined).map(_.get).toMap
//            state
//          }
//
//        state.update(Map(key -> newState))
//        if (timeOutType != GroupStateTimeout.NoTimeout()) {
//          state.setTimeoutTimestamp(expirationTime)
//        }
//
//        Map("active" -> activeCnt, "inActive" -> inActiveCnt, "companyId" -> key, "time" -> (new Date()).getTime)
//      }
//    }
//
//    val outputFrame = frame
//      .groupByKey(row => row.getAs[Long](operatorConfig.keyColumn))
//      .mapGroupsWithState(timeOutType)(eventTimeExpirationFunc).toDF()
//
//    new SparkOperatorResponse(true, Map(operatorContext.taskName -> outputFrame))
//  }
//
//}
