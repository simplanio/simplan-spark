/*
 * Copyright 2024, Intuit Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */
package com.intuit.data.simplan.spark.core.kafka

import com.intuit.data.simplan.core.domain.StreamingParseMode
import org.apache.spark.sql.types.StructType


/** @author Abraham, Thomas - tabraham1
  *         Created on 20-Sep-2024 at 12:04 PM
  */
case class FormatOptions(
    format: String,
    payloadSchema: Option[StructType]= None,
    headerSchema: Option[StructType] = None,
    headerField: Option[String] = Some("__event_header"),
    parseMode: Option[StreamingParseMode] = Some(StreamingParseMode.ALL_PARSED)
) extends Serializable{
  lazy val RESOLVED_PARSE_MODE: StreamingParseMode = parseMode.getOrElse(StreamingParseMode.PAYLOAD_ONLY)
  lazy val HEADER_FIELD: String = headerField.getOrElse("key")
}
