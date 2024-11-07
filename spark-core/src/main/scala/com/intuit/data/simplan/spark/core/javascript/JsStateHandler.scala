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
package com.intuit.data.simplan.spark.core.javascript

import com.intuit.data.simplan.common.scripting.js.{JSFunctions, SimplanJavaScriptScripting}
import com.intuit.data.simplan.global.utils.SimplanImplicits._
import com.intuit.data.simplan.spark.core.domain.{JsStateData, StateFieldSource}
import com.intuit.data.simplan.spark.core.operators.transformations.streaming.{JsMapGroupByStateOperator, SimplanMapGroupByStateContext}
import org.apache.spark.sql.Row

import java.lang
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.util.Try

/** @author Abraham, Thomas - tabraham1
  *         Created on 25-Oct-2024 at 7:43 PM
  */


class JsStateHandler(val context: SimplanMapGroupByStateContext) extends Serializable {
  @transient lazy val engine: SimplanJavaScriptScripting = new SimplanJavaScriptScripting(getJSFunctions)
  val stateUtils: StateManipulationUtils = new StateManipulationUtils()

  def executeRule(ruleName: String, eventMap: java.util.Map[String, AnyRef], state: JsStateData) = {
    val (actionFunctionName, filterFunctionName) = getRuleFunctionNames(ruleName)
    val conditionResult = engine.evaluateBooleanExpression(filterFunctionName, eventMap, state, stateUtils)
    if (conditionResult) engine.evaluateExpression(actionFunctionName, eventMap, state, stateUtils)
  }

  def constructReturnRow(inputRow: Row, stateData: JsStateData): String = {
    val data = mutable.Map[String, AnyRef]()
    context.operatorConfig.returns.foreach(returnField => {
      val fieldSource = JsMapGroupByStateOperator.getFieldSource(returnField, inputRow, context.operatorConfig.stateKeys)
      val fieldValue: AnyRef = fieldSource match {
        case StateFieldSource.INPUT   => Try(inputRow.getAs[AnyRef](returnField)).getOrElse(null)
        case StateFieldSource.LIST    => Try(stateData.getLists.get(returnField)).getOrElse(List.empty[String])
        case StateFieldSource.COUNTER => Try(stateData.getCounters.get(returnField).toString.toDouble.toLong.asInstanceOf[AnyRef]).getOrElse(new lang.Long(0))
        case StateFieldSource.UNKNOWN => null
      }
      data.put(returnField, fieldValue)
    })
    data.toMap.toJson
  }

  private def getJSFunctions: List[JSFunctions] = {
    val jsFunctions = new mutable.ListBuffer[JSFunctions]()
    context.stateRules.foreach {
      case (name, jsConfig) =>
        val (actionFunctionName, filterFunctionName) = getRuleFunctionNames(name)
        jsFunctions += JSFunctions(actionFunctionName, List("event", "state", "fn"), jsConfig.action)
        jsFunctions += JSFunctions(filterFunctionName, List("event", "state", "fn"), s"return (${jsConfig.condition})")
    }
    jsFunctions.toList
  }

  private def getRuleFunctionNames(ruleName: String): (String, String) = {
    val prefix = s"${context.operatorContext.taskName}_JsStateProcessFunction_${ruleName}"
    (s"${prefix}_action", s"${prefix}_condition")
  }

}
