package com.intuit.data.simplan.spark.core.utils

import com.intuit.data.simplan.logging.Logging
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 29-Nov-2021 at 10:29 AM
  */
object DebugUtils extends Logging {

  def printSchema(heading: String, dataframe: DataFrame): Unit =
    if (logger.isDebugEnabled()) {
      println(s"===== Start : $heading ======")
      dataframe.printSchema()
      println(s"===== End : $heading ======")

    }

}
