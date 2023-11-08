///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to you under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.intuit.data.simplan.spark.core.operators.sinks.stream
//
//import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
//import com.intuit.data.simplan.global.utils.SimplanImplicits.ToJsonImplicits
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
//import com.intuit.data.simplan.spark.core.handlers.foreachbatch.ForEachBatchHandler
//import com.intuit.data.simplan.spark.core.operators.SparkOperator
//
///**
//  * @author Abraham, Thomas - tabraham1
//  *         Created on 04-Sep-2022 at 9:48 AM
//  */
//@CaseClassDeserialize
//case class StreamingForEachBatchSinkConfig(source: String, forEachBatchHandler: String, handlerConfig: Option[AnyRef], outputMode: String, options: Map[String, String] = Map.empty)
//
//class ForEachBatchStreamingSink(sparkAppContext: SparkAppContext) extends SparkOperator(sparkAppContext) {
//
//  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
//    val operatorConfig: StreamingForEachBatchSinkConfig = request.parseConfigAs[StreamingForEachBatchSinkConfig]
//    val sourceFrame = request.dataframes(operatorConfig.source)
//    val handler: ForEachBatchHandler = Class.forName(operatorConfig.forEachBatchHandler).getConstructor(classOf[SparkAppContext]).newInstance(sparkAppContext).asInstanceOf[ForEachBatchHandler]
//    val forEachBatchFunction = handler.forEachBatch(request, operatorConfig.handlerConfig.getOrElse("{}").toJson)
//    sourceFrame.writeStream
//      .format("delta")
//      .outputMode(operatorConfig.outputMode)
//      .options(operatorConfig.options)
//      .foreachBatch(forEachBatchFunction)
//      .start()
//      .awaitTermination()
//    SparkOperatorResponse.continue
//  }
//}
