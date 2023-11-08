//package com.intuit.data.simplan.spark.core.operators.transformations
//
//import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
//import com.intuit.data.simplan.core.domain.TableType
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
//import com.intuit.data.simplan.spark.core.operators.SparkOperator
//import org.apache.spark.sql.DataFrame
//
///**
//  * @author - Abraham, Thomas - tabaraham1
//  *         Created on 8/19/21 at 3:01 PM
//  */
//
//@CaseClassDeserialize
//case class SqlProjectionOperatorConfig(source: String, projections: List[String], tableType: TableType = TableType.TEMP) extends Serializable
//
//class SqlProjectionOperator(appContext: SparkAppContext) extends SparkOperator(appContext) {
//
//  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
//    val sparkSqlConfig = request.parseConfigAs[SqlProjectionOperatorConfig]
//    val frame: DataFrame = appContext.spark.sql(sparkSqlConfig.source)
//    val frameWithExpressions = if (sparkSqlConfig.projections.nonEmpty) frame.selectExpr(sparkSqlConfig.projections: _*) else frame
//    if (sparkSqlConfig.tableType == TableType.TEMP) frameWithExpressions.createOrReplaceTempView(operatorContext.taskName)
//    new SparkOperatorResponse(true, Map(operatorContext.taskName -> frame))
//  }
//}
