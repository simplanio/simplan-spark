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

import com.intuit.data.simplan.core.domain.StreamingParseMode.{ALL_PARSED, HEADER_ONLY, PAYLOAD_ONLY}
import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import org.apache.spark.sql.functions.{col, current_timestamp, from_json, struct}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Column, DataFrame}

/** @author Abraham, Thomas - tabraham1
 *          Created on 20-Sep-2024 at 11:59 AM
 */

object KafkaDataframeFormatter {
  val PARSED_PAYLOAD_COLUMN = "__event_payload_parsed"
  val RAW_PAYLOAD_COLUMN = "__event_payload"
  val HEADER_COLUMN = "__event_headers"

  def formatKafkaMessage(dataFrame: DataFrame, formatOptions: FormatOptions): DataFrame = {
    implicit val formatingOptions: FormatOptions = formatOptions
    dataFrame
      .pipe(applyInitialCleanup)
      .pipe(parsePayload)
    //  .withColumn("__source_recorded_timestamp",col("server_enrichment.session.derived_timestamp"))
      .pipe(applyFinalCleanup)
  }

  private def applyInitialCleanup(dataframe: DataFrame): DataFrame = {
    val valuesToKeep = List("headers", "value")
    val fields = dataframe.schema.fields.map(_.name).filterNot(valuesToKeep.contains)
    val renamedDataframe = dataframe.withColumn("__meta", struct(fields.map(col): _*)).drop(fields: _*)

    renamedDataframe
      .withColumnRenamed("value", RAW_PAYLOAD_COLUMN)
      .withColumnRenamed("headers", HEADER_COLUMN)
      .withColumn("__created_at", col("__meta.timestamp"))
      .withColumn("__updated_at", col("__meta.timestamp"))
      .withColumn("__partition", col("__meta.partition"))
      .drop("__meta")
  }

  private def applyFinalCleanup(dataframe: DataFrame)(implicit config: FormatOptions): DataFrame = {
    val columns = dataframe.schema.fields.map(_.name).filter(_ != PARSED_PAYLOAD_COLUMN)
    val flattenedColumns: Array[Column] = (columns ++ Array(PARSED_PAYLOAD_COLUMN + ".*")).map(col)
    dataframe.select(flattenedColumns: _*)
  }

  private def parsePayload(dataframe: DataFrame)(implicit config: FormatOptions): DataFrame = {
    if (config.payloadSchema.isEmpty) return dataframe
    config.format.toUpperCase match {
      case "JSON" =>
        val column = from_json(col(RAW_PAYLOAD_COLUMN).cast(StringType), config.payloadSchema.get)
        dataframe
          .withColumn(PARSED_PAYLOAD_COLUMN, column)
      case _ => dataframe
    }
  }
}
