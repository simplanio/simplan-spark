package com.intuit.data.simplan.spark.launcher.spark

import com.intuit.data.simplan.core.context.DefaultRunParameters
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.service.SparkApplication

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 06-Oct-2021 at 12:58 AM
  */
object SimPlanSparkLauncherDelta extends App {

  private val userConfigs = Array("/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/simplan-config-ri/app/invoice-materialisation/invoice-materialisation.conf")
  val context = new SparkAppContext(userConfigs)
  SparkApplication(context).run(new DefaultRunParameters())
}
