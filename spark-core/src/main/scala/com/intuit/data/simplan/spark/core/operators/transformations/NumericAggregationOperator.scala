//package com.intuit.data.simplan.spark.core.operators.transformations
//
//import com.intuit.data.simplan.global.domain.QualifiedParam
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
//import com.intuit.data.simplan.spark.core.operators.SparkOperator
//
///**
//  * @author Abraham, Thomas - tabraham1
//  *         Created on 25-Apr-2022 at 8:00 PM
//  */
//case class NumericAggregationConfig(source: String, aggs: Map[String, QualifiedParam])
//
//class NumericAggregationOperator(sparkAppContext: SparkAppContext) extends SparkOperator(sparkAppContext) {
//
//  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
//    val config = request.parseConfigAs[NumericAggregationConfig]
//    val frame = request.dataframes(config.source)
//    val columns: List[String] = config.aggs.map(each => s"${each._2.qualifiedString} as ${each._1}").toList
//    val frame1 = frame.selectExpr(columns: _*)
//    new SparkOperatorResponse(true, Map(operatorContext.taskName -> frame1))
//  }
//}
