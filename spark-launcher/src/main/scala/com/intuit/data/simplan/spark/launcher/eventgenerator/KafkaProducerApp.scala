package com.intuit.data.simplan.spark.launcher.eventgenerator

import com.intuit.data.simplan.global.utils.SimplanImplicits._
import com.intuit.data.simplan.spark.launcher.eventgenerator.VisitorEvents.random
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.header.internals.RecordHeader

import java.time.Instant
import java.util
import scala.util.Try

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object KafkaProducerApp extends App {

  val props: util.Properties = new util.Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  val producer = new KafkaProducer[String, String](props)

  val topic = "simplan-ri-source"
  val headers: util.List[Header] = util.Arrays.asList(new RecordHeader("header_key", "header_value".getBytes))

  try {
    val data = """{ "id": 3, "name":{ "firstName": "Rama", "lastName": "Arvabhumi" }, "location": "Milpitas", "workingOn": "Simplan" }"""
      val record1 = new ProducerRecord[String, String](topic, data)
    record1.headers().add("changeAction", "header_value".getBytes)
    record1.headers().add("header_key1", "header_value1".getBytes)
      producer.send(record1)
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }

}
