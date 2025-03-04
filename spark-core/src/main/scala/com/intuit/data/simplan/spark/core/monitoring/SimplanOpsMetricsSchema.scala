/*
*  Copyright 2025, Intuit Inc
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
package com.intuit.data.simplan.spark.core.monitoring

import org.apache.spark.sql.types._

/**
 * @author Abraham, Thomas - tabraham1
 *         Created on 03-Mar-2025 at 1:18 PM
 */
object SimplanOpsMetricsSchema {

  private val errorStructType: StructType = StructType(Array(
    StructField("message", StringType, nullable = true),
    StructField("stackTrace", StringType, nullable = true),
    StructField("type", StringType, nullable = true),
    StructField("cause", StringType, nullable = true),
    StructField("causeTrace", StringType, nullable = true),
  ))
  private val taskType: StructType = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("index", LongType, nullable = true),
    StructField("operatorType", StringType, nullable = true),
    StructField("operator", StringType, nullable = true),
  ))
  private val processType: StructType = StructType(Array(
    StructField("name", StringType, nullable = true),
    StructField("start", TimestampType, nullable = true),
    StructField("end", TimestampType, nullable = true),
    StructField("duration", LongType, nullable = true),
    StructField("status", StringType, nullable = true)
  ))
  private val metaType: StructType = StructType(Array(
    StructField("asset", StringType, nullable = true),
    StructField("opsOwner", StringType, nullable = true),
    StructField("businessOwner", StringType, nullable = true)
  ))
  private val contextType: StructType = StructType(Array(
    StructField("appName", StringType, nullable = true),
    StructField("parentName", StringType, nullable = true),
    StructField("namespace", StringType, nullable = true),
    StructField("environment", StringType, nullable = true),
    StructField("runId", StringType, nullable = true),
    StructField("instanceId", StringType, nullable = true),
    StructField("subject", StringType, nullable = true),
    StructField("type", StringType, nullable = true),
    StructField("action", StringType, nullable = true),
    StructField("level", StringType, nullable = true),
    StructField("source", StringType, nullable = true),
    StructField("orchestrator", StringType, nullable = true),
    StructField("orchestratorId", StringType, nullable = true),
    StructField("applicationId", StringType, nullable = true)
  ))
  val opsEventSchema = StructType(Array(
    StructField("metricVersion", StringType, nullable = true),
    StructField("metricId", StringType, nullable = true),
    StructField("message", StringType, nullable = true),
    StructField("detailedMessage", StringType, nullable = true),
    StructField("@timestamp", TimestampType, nullable = true),
    StructField("labels", MapType(StringType, StringType), nullable = true),
    StructField("tags", ArrayType(StringType), nullable = true),
    StructField("error", errorStructType, nullable = true),
    StructField("task", taskType, nullable = true),
    StructField("process", processType, nullable = true),
    StructField("configDefinition", StringType, nullable = true),
    StructField("eventData", StringType, nullable = true),
    StructField("meta", metaType, nullable = true),
    StructField("context", contextType, nullable = true),
    StructField("processingTime", TimestampType, nullable = true),
    StructField("dataTime", TimestampType, nullable = true)
  ))
}
