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

/** @author Abraham, Thomas - tabraham1
  *         Created on 27-Dec-2023 at 10:17 AM
  */
case class SparkRetryConfig(
    enabled: Boolean = false,
    maxAttempts: Int = 3,
    delayInSeconds: Int = 5
) {
  def isRetryEnabled: Boolean = Option(enabled).getOrElse(false)
  def getMaxAttempts: Int = Option(maxAttempts).getOrElse(3)
  def getDelayInSeconds: Int = Option(delayInSeconds).getOrElse(5)
}
