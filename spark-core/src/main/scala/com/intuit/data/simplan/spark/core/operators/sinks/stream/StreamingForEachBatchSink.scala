//package com.intuit.data.simplan.spark.core.operators.sinks.stream
//
//import com.fasterxml.jackson.module.caseclass.annotation.CaseClassDeserialize
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.domain.{SparkForEachBatchFunctionOperatorResponse, SparkOperatorRequest, SparkOperatorResponse}
//import com.intuit.data.simplan.spark.core.operators.SparkOperator
//import org.apache.spark.sql.DataFrame
//
///**
//  * @author Kiran Hiremath
//  */
//
//@CaseClassDeserialize
//case class StreamingForEachBatchSinkConfig(source: String, forEachBatchFunction: String, outputMode: String, options: Map[String, String] = Map.empty)
//
//class StreamingForEachBatchSink(sparkAppContext: SparkAppContext) extends SparkOperator(sparkAppContext) {
//
//  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
//    val writerConfig: StreamingForEachBatchSinkConfig = request.parseConfigAs[StreamingForEachBatchSinkConfig]
//    val sourceFrame = request.dataframes(writerConfig.source)
//    val response: SparkOperatorResponse = request.sparkOperatorResponses(writerConfig.forEachBatchFunction)
//    val mergeOpResponse: SparkForEachBatchFunctionOperatorResponse = response.asInstanceOf[SparkForEachBatchFunctionOperatorResponse]
//    val function = mergeOpResponse.forEachBatchFunction
//    sourceFrame.writeStream
//      .format("delta")
//      .outputMode(writerConfig.outputMode)
//      .options(writerConfig.options)
//      .foreachBatch(function)
//      .start()
//      .awaitTermination()
//    SparkOperatorResponse.continue
//  }
//}
