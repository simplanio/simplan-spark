package com.intuit.data.simplan.spark.launcher.tests

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import java.io.File
import java.net.URI

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object Test3 {
  lazy val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    val str1= "/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/simplan-spark/spark-launcher/src/main/resources/spark-dev.conf"
    val str2= "s3:/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/simplan-spark/spark-launcher/src/main/resources/spark-dev.conf"
    val str3= "gcp:/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/simplan-spark/spark-launcher/src/main/resources/spark-dev.conf"
    val uri1 = new URI(str1)
    val uri2 = new URI(str2)
    val uri3 = new URI(str3)
    println("")

  }

}
