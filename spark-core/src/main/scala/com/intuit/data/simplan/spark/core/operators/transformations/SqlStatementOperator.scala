package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.SqlStatementConfig
import com.intuit.data.simplan.core.domain.{Lineage, TableType}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.DataFrame

/** @author - Abraham, Thomas - tabaraham1
  *         Created on 8/19/21 at 3:01 PM
  */
class SqlStatementOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[SqlStatementConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    appContext.emitters.get("opsMetrics").emit("Something is sending a message to opsMetrics")
    val frame: DataFrame = appContext.spark.sql(operatorConfig.sql)
    if (operatorConfig.tableType == TableType.TEMP) frame.createOrReplaceTempView(operatorConfig.table.getOrElse(operatorContext.taskName))
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> frame), Map.empty, Map(Lineage.RESPONSE_VALUE_KEY -> Lineage(operatorConfig.sql)))
  }
}
