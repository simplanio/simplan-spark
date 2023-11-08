package com.intuit.data.simplan.spark.launcher.tests

import com.intuit.data.simplan.spark.launcher.eventgenerator.{IVisitor, VisitorEvents}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.slf4j.LoggerFactory

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object Test {
  lazy val logger = LoggerFactory.getLogger(getClass)

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  spark
    .readStream
    .format("kafka")
    .options(Map(
      "kafka.bootstrap.servers" -> "eventbus-kafka.pl-data-lake-prd.a.intuit.com:19701,eventbus-kafka.pl-data-lake-prd.a.intuit.com:19801,eventbus-kafka.pl-data-lake-prd.a.intuit.com:19901",
      "kafka.security.protocol" -> "SSL",
      "kafka.ssl.enabled.protocols" -> "TLSv1.2",
      "startingOffsets" -> "latest",
      "failOnDataLoss" -> "false",
      "subscribe" -> "sg-test"
    ))
    .load()
    .selectExpr("CAST(value AS STRING) as payload")
    .selectExpr("from_json(payload, 'parent STRING, appName STRING, asset STRING, sqlScript STRING, businessOwner STRING, opsOwner STRING, sha STRING, environment STRING, source STRING') as records")
    .selectExpr("records.*")
    .select("parent", "appName", "asset")
    .groupBy("asset").agg(count("asset").as("count"))
    .select(to_json(struct("*")).as("value"))
    .selectExpr("CAST(value AS STRING)")
    .writeStream
    .trigger(Trigger.ProcessingTime("2 seconds"))
    .outputMode("update")
    .format("kafka")
    .options(Map(
      "kafka.bootstrap.servers" -> "eventbus-kafka.pl-data-lake-prd.a.intuit.com:19701,eventbus-kafka.pl-data-lake-prd.a.intuit.com:19801,eventbus-kafka.pl-data-lake-prd.a.intuit.com:19901",
      "kafka.security.protocol" -> "SSL",
      "kafka.ssl.enabled.protocols" -> "TLSv1.2",
      "startingOffsets" -> "latest",
      "failOnDataLoss" -> "false",
      "topic" -> "sg-test-agg",
      "checkpointLocation" -> "/simplan/dev/thomas/apps/code_test/_checkpoint"
    ))
    .start()
    .awaitTermination()
//  val dataframe = spark.read.json("/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/SimPlan/spark-launcher/src/main/resources/sample/address.json")
//
//  def schemaTest = {
//    val schema: StructType = dataframe.schema
//    println(schema.json)
//    schema.printTreeString()
//
//    println("============")
//    val json: String = schema.json
//    println(json)
//    val newSchema = DataType.fromJson(json).asInstanceOf[StructType]
//    newSchema.printTreeString()
//
//    val json2 =
//      """{
//        |    "type": "struct",
//        |    "fields": [
//        |        {
//        |            "name": "id",
//        |            "type": "string",
//        |            "nullable": true
//        |        },
//        |        {
//        |            "name": "location",
//        |            "type": "string",
//        |            "nullable": true
//        |        }
//        |    ]
//        |}""".stripMargin
//
//    val newSchema2 = DataType.fromJson(json2).asInstanceOf[StructType]
//    newSchema2.printTreeString()
//
//  }
//
//  def exprTest = {
//    val stringToFrame = Map("test" -> dataframe)
//    val parser = new SparkExpressionParser(stringToFrame)
////    println(parser.parseAll(parser.sparkNumericOperations, "$test.count"))
////    println(parser.parseAll(parser.sparkNumericOperations, "$test.sum(id)"))
////    println(parser.parseAll(parser.booleanOperation, "$test.sum(id) < $test.count"))
////    println(parser.parseAll(parser.booleanOperation, "$test.sum(id) > 10"))
////    println(parser.parseAll(parser.booleanOperation, "$test.sum(id) +  $test.count == 1"))
//
//    println(parser.parseAll(parser.numericOperations, "sum(test,id)"))
//    println(parser.parseAll(parser.numericOperations, "count(test)"))
//    println(parser.parseAll(parser.booleanOperations, "count(test) == 4"))
//
//  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Integrating Kafka")
      .master("local[2]")
      .getOrCreate()
    val list = (1 to 10).toList
    import spark.implicits._
    val frame = spark.sparkContext.parallelize(list).map(each => VisitorEvents.generate(IVisitor())).flatMap(x => x).sortBy(_.sorter).toDF
    frame.write.parquet("/Users/tabraham1/Intuit/Temp/EventGenerated/4")
    println(frame.schema.json)
    //val frame = spark.read.json("/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/simplan-spark/spark-launcher/src/main/resources/sample/address.json")
  }

//  case class Step1Result(name: String, age: String)
//  case class Step2Result(name: String, age: String)
//  case class Hello(name: String, age: Int)
//  class Hello2(name: String, age: String)
//
//  def main(args: Array[String]): Unit = {
//    import SimplanImplicits._
//    val hello = Hello("Thomas", 35)
//    val hello2 = new Hello2("Thomas", "35")
//    hello.getClass.getDeclaredFields.map(_.getName).foreach(println)
//    println(hello.productElement(1))
//    println(hello.isInstanceOf[Product])
//    println(hello2.isInstanceOf[Product])
//    val hi = hello.fromName[Int]("name")
//
//    val value = hello.productElement(0)
//    println(value.isInstanceOf[String])
//    //val value = hi.get
//    println(hi)
//    val json: String = Hello("Thomas", "35").toJson
//    val hello: Option[Hello] = json.fromJsonSafely[Hello]
//
//    retry(10) {
//      println("Hello")
//      throw new Exception()
//    }

  //println(SqlParser.splitScript(new File("/Users/tabraham1/Downloads/testSplit.sql")).toJson)

//  }

}
