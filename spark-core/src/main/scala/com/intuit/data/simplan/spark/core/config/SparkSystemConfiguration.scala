package com.intuit.data.simplan.spark.core.config

/** @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 3:19 PM
  */
case class SparkSystemConfiguration(
    options: Map[String, String] = Map.empty,
    properties: Map[String, String] = Map.empty,
    udfs: Map[String, SparkUdfConfiguration] = Map.empty)

object SparkSystemConfiguration {
  def empty = SparkSystemConfiguration()
}
