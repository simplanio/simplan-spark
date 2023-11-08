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

package com.intuit.data.simplan.spark.core.operators.sinks

import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class StreamingAwaitTerminationOperatorConfig(streams: List[String] = List.empty) extends OperatorConfig {
  val resolvedStreams: List[String] = if (streams != null) streams else List.empty
}

/** @author Abraham, Thomas - tabraham1
  *         Created on 15-Sep-2022 at 3:31 PM
  */
class AwaitStreamTermination(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[StreamingAwaitTerminationOperatorConfig](appContext, operatorContext) with Logging {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val futureStreams: List[Future[(String, Unit)]] =
      if (operatorConfig.resolvedStreams.isEmpty) List(Future("ALL", appContext.spark.streams.awaitAnyTermination()))
      else operatorConfig.streams.map(each => Future((each, appContext.spark.streams.active.filter(_.name == each).head.awaitTermination())))
    Await.ready(Future.sequence(futureStreams), Duration.Inf)
    SparkOperatorResponse.continue
  }

}
