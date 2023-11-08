package com.intuit.data.simplan.spark.launcher.tests

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import java.lang
import java.util.Properties
import scala.collection.JavaConverters.asJavaIterableConverter
case class DomainEvent(headers: Map[String, String], json: String, keys: String)

object KafkaProducerSparkApp extends App {
  // private val SOURCE_PATH = "/Users/tabraham1/Intuit/Development/WorkSpace/Simplan/DomainEvents/DomainEventsTest/InitialInput"
  // private val SOURCE_PATH = "/Users/tabraham1/Intuit/Development/WorkSpace/Simplan/DomainEvents/DomainEventsTest/IncrementalInput"
  private val SOURCE_PATH = "/Users/tabraham1/Intuit/Development/WorkSpace/Simplan/DomainEvents/DomainEventsTest/IncrementalInput_2"
  private val NUMBER_OF_EVENTS = 10
  private val TOPIC = "domain-events"

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  val producer = new KafkaProducer[String, String](props)

  val spark = SparkSession.builder()
    .appName("ProducingDomainEvents")
    .master("local[2]")
    .getOrCreate()

  private val rawDF: DataFrame = spark.read.json(SOURCE_PATH)

  import spark.implicits._

  val structToMap = udf((row: Row) => {
    row.schema.fieldNames
      .filter(field => !row.isNullAt(row.fieldIndex(field)))
      .map(field => field -> row.getAs[String](field))
      .toMap
  })

  val df = rawDF.withColumn("headers", structToMap($"headers"))
  private val domainEvents: Dataset[DomainEvent] = if (NUMBER_OF_EVENTS > 0) df.as[DomainEvent] /*.orderBy(rand())*/ .limit(NUMBER_OF_EVENTS) else df.as[DomainEvent]
  try {
    domainEvents.foreach(events => {
      val value = events.json
      val key = events.keys + "-" + events.headers.getOrElse("entityVersion", "empty")
      val headers: lang.Iterable[Header] = events.headers.map(each => new RecordHeader(each._1, each._2.getBytes).asInstanceOf[Header]).asJava
      val producerRecord = new ProducerRecord[String, String](TOPIC, null, key, value, headers)
      val metadata = producer.send(producerRecord)
      printf(
        s"sent record(key=%s) " +
          "meta(partition=%d, offset=%d)\n",
        producerRecord.key(),
        metadata.get().partition(),
        metadata.get().offset())
    })
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
