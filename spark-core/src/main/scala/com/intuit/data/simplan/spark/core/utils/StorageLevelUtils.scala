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

package com.intuit.data.simplan.spark.core.utils

import com.intuit.data.simplan.spark.core.domain.operator.config.SparkStorageLevel
import org.apache.spark.storage.StorageLevel

/** @author Abraham, Thomas - tabraham1
  *         Created on 12-Dec-2022 at 2:19 PM
  */
object StorageLevelUtils {

  def storageLevelMapper(storageLevel: SparkStorageLevel): StorageLevel = {
    storageLevel match {
      case SparkStorageLevel.NONE                  => StorageLevel.NONE
      case SparkStorageLevel.DISK_ONLY             => StorageLevel.DISK_ONLY
      case SparkStorageLevel.DISK_ONLY_2           => StorageLevel.DISK_ONLY_2
      case SparkStorageLevel.DISK_ONLY_3           => StorageLevel.DISK_ONLY_3
      case SparkStorageLevel.MEMORY_ONLY           => StorageLevel.MEMORY_ONLY
      case SparkStorageLevel.MEMORY_ONLY_2         => StorageLevel.MEMORY_ONLY_2
      case SparkStorageLevel.MEMORY_ONLY_SER       => StorageLevel.MEMORY_ONLY_SER
      case SparkStorageLevel.MEMORY_ONLY_SER_2     => StorageLevel.MEMORY_ONLY_SER_2
      case SparkStorageLevel.MEMORY_AND_DISK       => StorageLevel.MEMORY_AND_DISK
      case SparkStorageLevel.MEMORY_AND_DISK_2     => StorageLevel.MEMORY_AND_DISK_2
      case SparkStorageLevel.MEMORY_AND_DISK_SER   => StorageLevel.MEMORY_AND_DISK_SER
      case SparkStorageLevel.MEMORY_AND_DISK_SER_2 => StorageLevel.MEMORY_AND_DISK_SER_2
      case SparkStorageLevel.OFF_HEAP              => StorageLevel.OFF_HEAP
    }
  }
}
