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
package com.intuit.data.simplan.spark.core.operators.sinks.batch

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sinks.SparkBatchSinkConfig
import com.intuit.data.simplan.spark.core.operators.sinks.AbstractBatchSink

/** @author Abraham, Thomas - tabraham1
  *         Created on 07-Aug-2024 at 9:50 AM
  */

class IcebergBatchSink(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractBatchSink(sparkAppContext, operatorContext, Map.empty) {
  logger.info("IcebergFormatConfig: " + operatorConfig)
  logger.info("IcebergFormatConfigOverride: " + operatorConfig.resolvedSaveMode)
  override def customizeSourceConfig(readerConfig: SparkBatchSinkConfig): SparkBatchSinkConfig = readerConfig.copy(format = "iceberg")
}
