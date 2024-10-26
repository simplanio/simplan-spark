/*
 *  Copyright 2024, Intuit Inc
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.intuit.data.simplan.spark.core.operators.transformations.streaming

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{JsStateData, SparkOperatorRequest, SparkOperatorResponse, StateFieldSource}
import com.intuit.data.simplan.spark.core.javascript.JsStateHandler
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.utils.DataframeUtils
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
import org.apache.spark.sql.{DataFrame, Encoders, Row}

import scala.collection.JavaConverters._
import scala.collection.immutable.ListMap

/** @author Abraham, Thomas - tabraham1
  *         Created on 25-Oct-2024 at 2:19 PM
  */
/*case class StateData(
    counters: Map[String, Long],
    lists: Map[String, List[String]]
)*/

case class MapGroupByStateOperatorConfig(source: String, groupBy: List[String], stateHandler: String, stateRules: ListMap[String, JSStateRulesConfig], returns: List[String], stateKeys: StateKeysDefinition) extends OperatorConfig
case class StateKeysDefinition(counters: List[String] = List.empty, lists: List[String] = List.empty)
case class JSStateRulesConfig(condition: String, action: String)

class MapGroupByStateOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[MapGroupByStateOperatorConfig](appContext, operatorContext) {
  val jsStateHandler = new JsStateHandler(SimplanMapGroupByStateContext(operatorContext, operatorConfig.groupBy, operatorConfig.stateHandler, operatorConfig.stateRules, operatorConfig))
  val jsStateHandlerBC = appContext.spark.sparkContext.broadcast(jsStateHandler)

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val sourceDataframe: DataFrame = request.dataframes(operatorConfig.source)
    implicit val jsStateDataEncoder = Encoders.javaSerialization(classOf[JsStateData])
    import appContext.spark.implicits._
    val stateCalculated = sourceDataframe
      .withWatermark("timestamp", "10 seconds")
      .groupByKey(row => operatorConfig.groupBy.map(each => row.getAs(each).toString))
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.NoTimeout())(stateUpdateFunctionMapGroupsWithState)
    stateCalculated.printSchema()
    SparkOperatorResponse.continue(operatorContext.taskName, stateCalculated.toDF())
  }

  def initialState(): JsStateData = {
    new JsStateData(operatorConfig.stateKeys.lists.asJava, operatorConfig.stateKeys.counters.asJava)
  }

  def stateUpdateFunctionMapGroupsWithState(key: List[String], values: Iterator[Row], state: GroupState[JsStateData]): Iterator[String] = {
    val stateData = state.getOption.getOrElse(initialState())
    values.map(row => {
      val eventMap = DataframeUtils.rowToMap(row)
      jsStateHandlerBC.value.context.stateRules.foreach {
        case (ruleName, _) => jsStateHandlerBC.value.executeRule(ruleName, eventMap, stateData)
      }
      state.update(stateData)
      jsStateHandlerBC.value.constructReturnRow(row, stateData)
    })
  }

}

object MapGroupByStateOperator {

  def getFieldSource(fieldName: String, row: Row, stateKeys: StateKeysDefinition): StateFieldSource = {
    if (row.schema.fields.map(_.name).contains(fieldName)) StateFieldSource.INPUT
    else if (stateKeys.lists.contains(fieldName)) StateFieldSource.LIST
    else if (stateKeys.counters.contains(fieldName)) StateFieldSource.COUNTER
    else StateFieldSource.UNKNOWN
  }

}
