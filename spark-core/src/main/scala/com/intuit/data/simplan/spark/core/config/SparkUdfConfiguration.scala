package com.intuit.data.simplan.spark.core.config

import org.apache.commons.lang.StringUtils

/** @author Abraham, Thomas - tabraham1
  *         Created on 17-Mar-2022 at 9:19 AM
  */
case class SparkUdfConfiguration(className: String, resourceUri: Option[String], resourceType: Option[String]) {

  def functionDefinition(functionName: String): String = {
    val resourceLocations =
      if (resourceUri.isDefined)
        s"USING ${resourceType.getOrElse("JAR").toUpperCase} '${resourceUri.get}'"
      else StringUtils.EMPTY
    s"""CREATE OR REPLACE TEMPORARY FUNCTION $functionName AS '$className' $resourceLocations"""
  }
}
