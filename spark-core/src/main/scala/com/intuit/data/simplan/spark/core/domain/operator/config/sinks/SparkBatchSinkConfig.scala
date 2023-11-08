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

package com.intuit.data.simplan.spark.core.domain.operator.config.sinks

import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
import com.intuit.data.simplan.core.domain.operator.config.sinks.BatchSinkConfig
import com.intuit.data.simplan.spark.core.domain.operator.config.SparkRepartitionConfig

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 10:50 AM
  */
@CaseClassDeserialize
case class SparkBatchSinkConfig(
    override val source: String,
    override val format: String,
    override val location: String,
    override val options: Map[String, String] = Map.empty,
    repartition: Option[SparkRepartitionConfig] = None,
    partitionBy: List[String] = List.empty
) extends BatchSinkConfig(source, format, location, options) {
  def resolvedRepartition: SparkRepartitionConfig = repartition.getOrElse(SparkRepartitionConfig()).copy(hint = Option(location))

  def resolvedPartitionBy: List[String] = if (partitionBy != null) partitionBy else List.empty
}
