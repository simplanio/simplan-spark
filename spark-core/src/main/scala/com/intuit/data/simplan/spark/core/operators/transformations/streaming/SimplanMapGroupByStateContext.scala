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

import com.intuit.data.simplan.core.domain.operator.OperatorContext

import scala.collection.immutable.ListMap

/** @author Abraham, Thomas - tabraham1
  *         Created on 25-Oct-2024 at 11:28 PM
  */
case class SimplanMapGroupByStateContext(
                                          operatorContext: OperatorContext,
                                          groupBy: List[String],
                                          stateHandler: String,
                                          stateRules: ListMap[String, JSStateRulesConfig],
                                          operatorConfig: MapGroupByStateOperatorConfig
) extends Serializable