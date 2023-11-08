package com.intuit.data.simplan.spark.launcher

import com.intuit.data.simplan.core.context.DefaultRunParameters
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.service.SparkApplication

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 06-Oct-2021 at 12:58 AM
  */
object SimPlanSparkLauncher {
  def main(args: Array[String]): Unit = {
    val context = new SparkAppContext(args)
    SparkApplication(context).run(new DefaultRunParameters())
  }
}