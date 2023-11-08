package com.intuit.data.simplan.spark.launcher.tests

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.common.header.Header

import java.util.Properties

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object KafkaProducerApp extends App {

  val props: Properties = new Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  val producer = new KafkaProducer[String, String](props)

  import org.apache.kafka.common.header.internals.RecordHeader

  import java.util

  val topic = "text_topic"

  import org.apache.kafka.clients.producer.ProducerRecord

  val value =
    """{"meta":{"created":"2021-11-25T22:13:22Z","createdByUser":{"id":"9130356301481556"},"updated":"2021-11-25T22:13:22Z","updatedByUser":{"id":"9130356301481556"},"updatedByApp":{"id":"System"}},"id":{"entityId":"1","accountId":"9130356301481496"},"version":0,"sourceEntityVersion":0,"active":true,"type":"INVOICE","contactId":"1","accountId":"72","amount":"32904.5000000","homeAmount":"32904.5000000","memo":"J","referenceNumber":"5.04","lines":[{"sequence":11,"lineOrder":11,"description":"8ZAHFQq","quantity":"-1.0000000","rate":"1000.0000000","amount":"-1000.0000000","homeAmount":"-1000.0000000","netAmount":"-1000.0000000","taxAmount":"-99.7500000","accountId":"9","productVariantId":"3"},{"sequence":22,"lineOrder":22,"rate":"15.0000000","amount":"-15.0000000","homeAmount":"-15.0000000","netAmount":"-100.0000000","taxAmount":"-15.0000000","accountId":"60"},{"sequence":12,"lineOrder":12,"description":"cUOa","quantity":"-1.0000000","rate":"1000.0000000","amount":"-1000.0000000","homeAmount":"-1000.0000000","netAmount":"-1000.0000000","taxAmount":"-130.0000000","accountId":"9","productVariantId":"3"},{"sequence":13,"lineOrder":13,"description":"pWC","quantity":"-1.0000000","rate":"100.0000000","amount":"-100.0000000","homeAmount":"-100.0000000","netAmount":"-100.0000000","taxAmount":"-15.0000000","accountId":"9","productVariantId":"3"},{"sequence":14,"lineOrder":14,"rate":"7.0000000","amount":"-560.0000000","homeAmount":"-560.0000000","netAmount":"-8000.0000000","taxAmount":"-560.0000000","accountId":"62"},{"sequence":15,"lineOrder":15,"rate":"5.0000000","amount":"-100.0000000","homeAmount":"-100.0000000","netAmount":"-2000.0000000","taxAmount":"-100.0000000","accountId":"68"},{"sequence":16,"lineOrder":16,"rate":"8.0000000","amount":"-800.0000000","homeAmount":"-800.0000000","netAmount":"-10000.0000000","taxAmount":"-800.0000000","accountId":"62"},{"sequence":17,"lineOrder":17,"rate":"5.0000000","amount":"-550.0000000","homeAmount":"-550.0000000","netAmount":"-11000.0000000","taxAmount":"-550.0000000","accountId":"60"},{"sequence":18,"lineOrder":18,"rate":"9.9750000","amount":"-199.5000000","homeAmount":"-199.5000000","netAmount":"-2000.0000000","taxAmount":"-199.5000000","accountId":"70"},{"sequence":19,"lineOrder":19,"rate":"7.0000000","amount":"-210.0000000","homeAmount":"-210.0000000","netAmount":"-3000.0000000","taxAmount":"-210.0000000","accountId":"65"},{"sequence":0,"lineOrder":0,"description":"J","amount":"32904.5000000","homeAmount":"32904.5000000","netAmount":"30100.0000000","taxAmount":"2804.5000000","accountId":"72"},{"sequence":1,"lineOrder":1,"description":"m4PFCldVt","quantity":"-1.0000000","rate":"2000.0000000","amount":"-2000.0000000","homeAmount":"-2000.0000000","netAmount":"-2000.0000000","taxAmount":"-100.0000000","accountId":"9","productVariantId":"3"},{"sequence":2,"lineOrder":2,"description":"L","quantity":"-1.0000000","rate":"3000.0000000","amount":"-3000.0000000","homeAmount":"-3000.0000000","netAmount":"-3000.0000000","taxAmount":"-360.0000000","accountId":"9","productVariantId":"3"},{"sequence":3,"lineOrder":3,"description":"qeXzre","quantity":"-1.0000000","rate":"4000.0000000","amount":"-4000.0000000","homeAmount":"-4000.0000000","netAmount":"-4000.0000000","taxAmount":"-520.0000000","accountId":"9","productVariantId":"3"},{"sequence":4,"lineOrder":4,"description":"97jesmxHj","quantity":"-1.0000000","rate":"5000.0000000","amount":"-5000.0000000","homeAmount":"-5000.0000000","netAmount":"-5000.0000000","taxAmount":"-350.0000000","accountId":"9","productVariantId":"3"},{"sequence":5,"lineOrder":5,"description":"N","quantity":"-1.0000000","rate":"6000.0000000","amount":"-6000.0000000","homeAmount":"-6000.0000000","netAmount":"-6000.0000000","taxAmount":"-480.0000000","accountId":"9","productVariantId":"3"},{"sequence":6,"lineOrder":6,"description":"p","quantity":"-1.0000000","rate":"1000.0000000","amount":"-1000.0000000","homeAmount":"-1000.0000000","netAmount":"-1000.0000000","taxAmount":"-120.0000000","accountId":"9","productVariantId":"3"},{"sequence":7,"lineOrder":7,"description":"HEA","quantity":"-1.0000000","rate":"2000.0000000","amount":"-2000.0000000","homeAmount":"-2000.0000000","netAmount":"-2000.0000000","taxAmount":"-240.0000000","accountId":"9","productVariantId":"3"},{"sequence":8,"lineOrder":8,"description":"R4mpA","quantity":"-1.0000000","rate":"2000.0000000","amount":"-2000.0000000","homeAmount":"-2000.0000000","netAmount":"-2000.0000000","taxAmount":"-140.0000000","accountId":"9","productVariantId":"3"},{"sequence":9,"lineOrder":9,"description":"onbTpF2yb","quantity":"-1.0000000","rate":"2000.0000000","amount":"-2000.0000000","homeAmount":"-2000.0000000","netAmount":"-2000.0000000","taxAmount":"-100.0000000","accountId":"9","productVariantId":"3"},{"sequence":20,"lineOrder":20,"rate":"12.0000000","amount":"-240.0000000","homeAmount":"-240.0000000","netAmount":"-2000.0000000","taxAmount":"-240.0000000","accountId":"60"},{"sequence":10,"lineOrder":10,"description":"L0RZ","quantity":"-1.0000000","rate":"1000.0000000","amount":"-1000.0000000","homeAmount":"-1000.0000000","netAmount":"-1000.0000000","taxAmount":"-149.7500000","accountId":"9","productVariantId":"3"},{"sequence":21,"lineOrder":21,"rate":"13.0000000","amount":"-130.0000000","homeAmount":"-130.0000000","netAmount":"-1000.0000000","taxAmount":"-130.0000000","accountId":"60"}],"addresses":[],"balance":"32904.5000000"}"""
  val key = """123146993889054-1777""".stripMargin
  val headers: util.List[Header] = util.Arrays.asList(new RecordHeader("header_key", "header_value".getBytes))

  try {
    val record1 = new ProducerRecord[String, String](topic, 0, "key", "value", headers)
    val metadata = producer.send(record1)
    printf(
      s"sent record(key=%s value=%s) " +
        "meta(partition=%d, offset=%d)\n",
      record1.key(),
      record1.value(),
      metadata.get().partition(),
      metadata.get().offset())
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }
}
