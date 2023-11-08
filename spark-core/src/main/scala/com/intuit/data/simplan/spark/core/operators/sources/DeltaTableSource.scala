package com.intuit.data.simplan.spark.core.operators.sources

import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
import com.intuit.data.simplan.core.domain.TableType
import com.intuit.data.simplan.core.domain.operator.{OperatorConfig, OperatorContext}
import com.intuit.data.simplan.global.exceptions.SimplanExecutionException
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import io.delta.tables.DeltaTable
import org.apache.spark.sql.Row

/** @author Abraham, Thomas - tabraham1
  *         Created on 02-Dec-2021 at 2:10 PM
  */
@CaseClassDeserialize
case class DeltaTableSourceConfig(path: String, schema: Option[String], tableType: TableType = TableType.NONE, autoCreate: Boolean = true) extends OperatorConfig

class DeltaTableSource(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[DeltaTableSourceConfig](sparkAppContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    if (operatorConfig.autoCreate && !sparkAppContext.fileUtils.exists(operatorConfig.path)) {
      val frame = request.dataframes(operatorConfig.schema.get)
      sparkAppContext.spark.createDataFrame(sparkAppContext.sc.emptyRDD[Row], frame.schema).write.format("delta").save(operatorConfig.path)
    }
    if (DeltaTable.isDeltaTable(operatorConfig.path)) {
      val table = DeltaTable.forPath(operatorConfig.path)
      table.alias(operatorContext.taskName)
      new SparkOperatorResponse(
        canContinue = true,
        deltaTable = Map(operatorContext.taskName -> table)
      )
    } else {
      throw new SimplanExecutionException(s"${operatorConfig.path} is not a delta table. Exiting...")
    }
  }
}
