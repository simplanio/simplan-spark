package com.intuit.data.simplan.spark.launcher.tests

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.util.Properties
import scala.collection.JavaConverters._

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object KafkaConsumerSubscribeApp extends App {

  val props: Properties = new Properties()
  props.put("group.id", "test")
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  val consumer = new KafkaConsumer(props)
  val topics = List("topic_text")
  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(10)
      for (record <- records.asScala) {
        println(
          "Topic: " + record.topic() +
            ",Key: " + record.key() +
            ",Value: " + record.value() +
            ", Offset: " + record.offset() +
            ", Partition: " + record.partition())
      }
    }
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    consumer.close()
  }
}
