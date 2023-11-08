package com.intuit.data.simplan.spark.core.domain.operator.config.sources

import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
import com.intuit.data.simplan.core.domain.operator.config.sources.{StreamSourceConfig, WaterMarkConfig}
import com.intuit.data.simplan.core.domain.{StreamingParseMode, TableType}
import com.intuit.data.simplan.global.domain.QualifiedParam

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Nov-2021 at 3:00 PM
  */
@CaseClassDeserialize
case class SparkStreamSourceConfig(
    override val format: String,
    override val payloadSchema: Option[QualifiedParam],
    override val headerSchema: Option[QualifiedParam] = None,
    override val headerField: Option[String] = Some("key"),
    override val tableType: TableType = TableType.TEMP,
    override val table: Option[String] = None,
    override val parseMode: Option[StreamingParseMode] = Some(StreamingParseMode.PAYLOAD_ONLY),
    override val options: Map[String, String] = Map.empty,
    override val watermark: Option[WaterMarkConfig] = None
) extends StreamSourceConfig(format, payloadSchema, headerSchema, headerField, table, tableType, parseMode, options, watermark)
