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

package com.intuit.data.simplan.spark.core.domain.operator.config

import com.intuit.data.simplan.core.util.{PartitionOptimizerResult, PartitionOptimizerUtils}
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.utils.DataframeUtils
import org.apache.spark.sql.SparkSession

/** @author Abraham, Thomas - tabraham1
  *         Created on 13-Jul-2023 at 5:56 PM
  */
case class SparkRepartitionConfig(
    enabled: Boolean = true,
    numberOfPartitions: Option[Int] = None, // Forced Partitions
    columns: List[String] = List.empty,
    hint: Option[String] = None,
    optimalFileSize: Option[Long] = Some(128L),
    defaultNumOfPartitions: Option[Int] = None
) extends Logging {
  val resolvedColumns: List[String] = if (columns == null || columns.isEmpty) List.empty else columns

  def calculateOptimisedPartition(spark: SparkSession): Option[PartitionOptimizerResult] = {
    if (!enabled) {
      logger.info(s"Repartition is disabled. Using Spark default partition")
      return None
    }
    //If number of partitions is defined, return it
    if (numberOfPartitions.isDefined) {
      logger.info(s"Number of partitions is defined as ${numberOfPartitions.get}")
      return Option(PartitionOptimizerResult(numberOfPartitions))
    }
    if (hint.isEmpty) {
      logger.info(s"Hint is not defined. Use Spark default partition")
      return None
    }

    val location = if (isHintTableName) DataframeUtils.getLocationOfTable(spark, hint.get) else hint

    if (location.isEmpty) {
      logger.info(s"Not able to find Location for hint ${hint.get}. Use Spark default partition")
      return None
    }

    Option(PartitionOptimizerUtils.reCalculatePartitionCount(location.get, optimalFileSize.getOrElse(128L).toString.toLong, defaultNumOfPartitions))
  }
  def isHintTableName: Boolean = hint.isDefined && hint.contains(".") && !hint.get.contains("/")

}
