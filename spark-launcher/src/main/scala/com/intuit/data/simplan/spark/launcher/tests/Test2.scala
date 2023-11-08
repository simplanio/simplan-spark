package com.intuit.data.simplan.spark.launcher.tests

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object Test2 {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Integrating Kafka")
      .master("local[2]")
      .getOrCreate()
    import io.delta.tables._
    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
    val deltaTablePath = "/Users/tabraham1/Intuit/Development/WorkSpace/Simplan/DseUsecase/VendorsMaster"
    if (!new File(deltaTablePath).exists()) {
      spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema).write.format("delta").save(deltaTablePath)
    }
    val vendorsMaster: DeltaTable = DeltaTable.forPath(spark, deltaTablePath)
    val dfUpdates: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "vendorMaster")
      .option("startingOffsets", "earliest") // From starting
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .select("data.*")

    vendorsMaster
      .as("vendorMaster")
      .merge(dfUpdates.as("updates"), "vendorMaster.id = updates.id")
      .whenMatched
      .updateAll()
      .whenNotMatched
      .insertAll()
      .execute()

  }

}
