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

package com.intuit.data.simplan.spark.core.operators.sinks.stream

import com.intuit.data.simplan.core.domain.operator.config.SinkConfig
import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse, TriggerModes}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.{ForeachWriter, Row}

case class StreamingForEachSinkConfig(source: String, checkpointLocation: String, outputMode: String, sink: SinkConfig, options: Map[String, String] = Map.empty) extends OperatorConfig

abstract class AbstractForEachStreamSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[StreamingForEachSinkConfig](sparkAppContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {

    val triggerMode = TriggerModes("default", 0)
    val sourceFrame = request.dataframes(operatorConfig.source)
    sourceFrame.writeStream
      .outputMode(operatorConfig.outputMode)
      .option("checkpointLocation", operatorConfig.checkpointLocation)
      .trigger(triggerMode)
      .foreach(getImplClass(operatorConfig))
      .start()
      .awaitTermination()
    SparkOperatorResponse.continue
  }

  def getImplClass(streamingForEachSinkConfig: StreamingForEachSinkConfig): ForeachWriter[Row]

}
