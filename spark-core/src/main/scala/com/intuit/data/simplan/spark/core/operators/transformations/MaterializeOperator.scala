//package com.intuit.data.simplan.spark.core.operators.transformations
//
//import java.util.concurrent.TimeUnit
//import com.intuit.data.simplan.core.domain.operator.config.MaterializeOperatorConfig
//import com.intuit.data.simplan.spark.core.context.SparkAppContext
//import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
//import com.intuit.data.simplan.spark.core.operators.{SparkOperator, SparkOperatorContext}
//import com.intuit.data.simplan.spark.core.utils.DomainEventsMaterializer
//import io.delta.tables.DeltaTable
//import org.apache.spark.sql.streaming.Trigger
//import org.apache.spark.sql.{DataFrame, SparkSession}
//
//class MaterializeOperator(appContext: SparkAppContext,operatorContext: OperatorContext) extends SparkOperator(appContext,operatorContext) {
//
//  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
//
//    val config: MaterializeOperatorConfig = request.parseConfigAs[MaterializeOperatorConfig]
//    val streamDataFrame = request.dataframes(config.streamDataFrame)
//    //val batchDataFrame = request.dataframes(config.batchDataFrame)
//    val snapshotPath = config.snapshotPath
//
//    implicit val sparkSession: SparkSession = appContext.spark
//    implicit val domainEventsMaterializer: DomainEventsMaterializer = new DomainEventsMaterializer(snapshotPath)
//
//    val writeStream = streamDataFrame.writeStream
//      .option("checkpointLocation", "/Users/sambekar/warehouse/live_table/checkpoint1")
//      .option("queryName", s"icp-live-tables-domain-events")
//      .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
//      // .outputMode("update")
//      .foreachBatch { (batchDf: DataFrame, batchId: Long) =>
//        domainEventsMaterializer.materialize(batchDf)
//      }.start()
//
//    writeStream.awaitTermination()
//
//    SparkOperatorResponse.continue
//  }
//
//  /*def materialize(streamingDataFrame: DataFrame, snapshotTablePath: String)(implicit domainEventsMaterializer: DomainEventsMaterializer): Unit = {
//
//    val formattedEvents = domainEventsMaterializer.formatEvents(streamingDataFrame)
//    if (deltaTableExists(snapshotTablePath )) {
//
//      domainEventsMaterializer.mergeEventsIntoDeltaTable(formattedEvents)
//    } else {
//
//      domainEventsMaterializer.createDeltaTableFromEvents(formattedEvents)
//    }
//  }*/
//
//  private def deltaTableExists(snapshotTablePath: String): Boolean = {
//
//    DeltaTable.isDeltaTable(snapshotTablePath)
//  }
//
//}
