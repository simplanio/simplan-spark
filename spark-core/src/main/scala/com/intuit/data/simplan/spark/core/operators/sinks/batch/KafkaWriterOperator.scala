//package com.intuit.data.simplan.spark.core.operators.write
//
//import com.intuit.data.simplan.global.utils.JacksonImplicits.StringImplicits
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.operators.{SparkOperator, SparkOperatorRequest, SparkOperatorResponse}
//import org.apache.spark.sql.functions.{struct, to_json}
//
///**
//  * @author - Abraham, Thomas - tabaraham1
//  *         Created on 8/20/21 at 12:07 PM
//  */
//
//case class EventBusSycConfig(table: String, topic: String)
//
//class KafkaWriterOperator(sparkAppContext: SparkAppContext) extends SparkOperator(sparkAppContext) {
//
//  override def process(input: SparkOperatorRequest): SparkOperatorResponse = {
//
//    val eventBusConfig = input.operatorDefinition.config.fromJson[EventBusSycConfig]
//    input.dataframes(eventBusConfig.table)
//      .select(to_json(struct("*")).as("value"))
//      .selectExpr("CAST(value AS STRING)")
//      .write
//      .format("kafka")
//      .option("kafka.bootstrap.servers", "localhost:9092")
//      .option("topic", eventBusConfig.topic)
//      .save()
//    SparkOperatorResponse.continue
//  }
//
//}
