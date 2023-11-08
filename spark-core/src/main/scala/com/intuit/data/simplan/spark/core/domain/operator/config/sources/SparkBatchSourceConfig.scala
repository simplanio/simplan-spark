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

package com.intuit.data.simplan.spark.core.domain.operator.config.sources

import com.intuit.data.simplan.core.domain.TableType
import com.intuit.data.simplan.core.domain.operator.config.sources.BatchSourceConfig
import com.intuit.data.simplan.global.domain.QualifiedParam

/** @author Abraham, Thomas - tabraham1
  *         Created on 12-Dec-2022 at 10:10 AM
  */
case class SparkBatchSourceConfig(
    override val location: String,
    override val format: String,
    override val schema: Option[QualifiedParam],
    override val tableType: TableType = TableType.NONE,
    override val table: Option[String] = None,
    override val projection: List[String] = List.empty,
    override val filter: Option[String] = None,
    override val options: Map[String, String] = Map.empty,
    override val directorySortPattern: Option[String] = None
) extends BatchSourceConfig(location, format, schema, tableType, table, projection, filter, options, directorySortPattern) {

}
