package com.intuit.data.simplan.spark.launcher.tests

import com.intuit.data.simplan.global.utils.SimplanImplicits.Pipe
import com.intuit.data.simplan.spark.core.domain.Constants._
import org.apache.spark.sql.functions.{col, from_json, udf}
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

class DomainEventsConvertor(spark: SparkSession) {

  def parseHeader(df: DataFrame) = {
    val ArrayToMapUdf = udf[Map[String, String], String] { array =>
      array.substring(2, array.length - 2).split("}, \\{")
        .map(each => {
          val splittedString = each.split(",")
          (splittedString(0).trim, splittedString(1).trim)
        }).toMap
    }
    df.withColumn(ColumnHeaders, ArrayToMapUdf(col(ColumnHeaders)))
  }

  def parseBody(dataFrame: DataFrame): DataFrame = {
    import spark.implicits._
    val bodySchema =
      """{"type":"struct","fields":[{"name":"footnote","type":"string","nullable":true,"metadata":{}},{"name":"addresses","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"addressId","type":"string","nullable":true,"metadata":{}},{"name":"freeFormAddressLine","type":"string","nullable":true,"metadata":{}},{"name":"s42Address","type":{"type":"struct","fields":[{"name":"house","type":"string","nullable":true,"metadata":{}},{"name":"category","type":"string","nullable":true,"metadata":{}},{"name":"near","type":"string","nullable":true,"metadata":{}},{"name":"house_number","type":"string","nullable":true,"metadata":{}},{"name":"road","type":"string","nullable":true,"metadata":{}},{"name":"unit","type":"string","nullable":true,"metadata":{}},{"name":"level","type":"string","nullable":true,"metadata":{}},{"name":"staircase","type":"string","nullable":true,"metadata":{}},{"name":"entrance","type":"string","nullable":true,"metadata":{}},{"name":"po_box","type":"string","nullable":true,"metadata":{}},{"name":"postcode","type":"string","nullable":true,"metadata":{}},{"name":"postcode_extension","type":"string","nullable":true,"metadata":{}},{"name":"suburb","type":"string","nullable":true,"metadata":{}},{"name":"city_district","type":"string","nullable":true,"metadata":{}},{"name":"city","type":"string","nullable":true,"metadata":{}},{"name":"island","type":"string","nullable":true,"metadata":{}},{"name":"state_district","type":"string","nullable":true,"metadata":{}},{"name":"state","type":"string","nullable":true,"metadata":{}},{"name":"country_region","type":"string","nullable":true,"metadata":{}},{"name":"country","type":"string","nullable":true,"metadata":{}},{"name":"world_region","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"geoLocation","type":{"type":"struct","fields":[{"name":"latitude","type":"string","nullable":true,"metadata":{}},{"name":"longitude","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"geocodeStatus","type":"string","nullable":true,"metadata":{}},{"name":"variation","type":{"type":"struct","fields":[{"name":"usage","type":"string","nullable":true,"metadata":{}},{"name":"purpose","type":"string","nullable":true,"metadata":{}},{"name":"ordinal","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"inputSource","type":{"type":"struct","fields":[{"name":"sourceSystem","type":"string","nullable":true,"metadata":{}},{"name":"inputTime","type":"timestamp","nullable":true,"metadata":{}},{"name":"inputMethod","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"verification","type":{"type":"struct","fields":[{"name":"verificationStatus","type":"string","nullable":true,"metadata":{}},{"name":"verificationTime","type":"date","nullable":true,"metadata":{}},{"name":"verificationMethod","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}},{"name":"type","type":"string","nullable":true,"metadata":{}},{"name":"contactId","type":"string","nullable":true,"metadata":{}},{"name":"accountId","type":"string","nullable":true,"metadata":{}},{"name":"amount","type":"string","nullable":true,"metadata":{}},{"name":"homeAmount","type":"string","nullable":true,"metadata":{}},{"name":"currency","type":"string","nullable":true,"metadata":{}},{"name":"memo","type":"string","nullable":true,"metadata":{}},{"name":"locationId","type":"string","nullable":true,"metadata":{}},{"name":"referenceNumber","type":"string","nullable":true,"metadata":{}},{"name":"lines","type":{"type":"array","elementType":{"type":"struct","fields":[{"name":"sequence","type":"integer","nullable":true,"metadata":{}},{"name":"lineOrder","type":"integer","nullable":true,"metadata":{}},{"name":"description","type":"string","nullable":true,"metadata":{}},{"name":"quantity","type":"string","nullable":true,"metadata":{}},{"name":"rate","type":"string","nullable":true,"metadata":{}},{"name":"amount","type":"string","nullable":true,"metadata":{}},{"name":"homeAmount","type":"string","nullable":true,"metadata":{}},{"name":"netAmount","type":"string","nullable":true,"metadata":{}},{"name":"taxAmount","type":"string","nullable":true,"metadata":{}},{"name":"accountId","type":"string","nullable":true,"metadata":{}},{"name":"productVariantId","type":"string","nullable":true,"metadata":{}}]},"containsNull":true},"nullable":true,"metadata":{}},{"name":"id","type":{"type":"struct","fields":[{"name":"entityId","type":"string","nullable":true,"metadata":{}},{"name":"accountId","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"version","type":"integer","nullable":true,"metadata":{}},{"name":"sourceEntityVersion","type":"integer","nullable":true,"metadata":{}},{"name":"active","type":"boolean","nullable":true,"metadata":{}},{"name":"meta","type":{"type":"struct","fields":[{"name":"owner","type":{"type":"struct","fields":[{"name":"id","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"created","type":"timestamp","nullable":true,"metadata":{}},{"name":"createdByUser","type":{"type":"struct","fields":[{"name":"id","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"updated","type":"timestamp","nullable":true,"metadata":{}},{"name":"updatedByUser","type":{"type":"struct","fields":[{"name":"id","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"updatedByApp","type":{"type":"struct","fields":[{"name":"id","type":"string","nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}}]},"nullable":true,"metadata":{}},{"name":"balance","type":"string","nullable":true,"metadata":{}}]}"""
    val structType = DataType.fromJson(bodySchema).asInstanceOf[StructType]
    dataFrame.withColumn("json", from_json($"json", structType))
      .withColumnRenamed("json", "payload")
      .withColumnRenamed("keys", "key")
  }
}

object DomainEventsConvertor extends App {

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()
  val dataframe = spark.read.json("/Users/tabraham1/Intuit/Development/WorkSpace/Simplan/DomainEvents/KafkaDump-Nov29-2021")

  private val convertor = new DomainEventsConvertor(spark)
  private val frame: DataFrame = convertor.parseHeader(dataframe) | convertor.parseBody
  frame.write.mode(SaveMode.Overwrite).json("/Users/tabraham1/Intuit/Development/WorkSpace/Simplan/Workspace/liveTableSource")
}
