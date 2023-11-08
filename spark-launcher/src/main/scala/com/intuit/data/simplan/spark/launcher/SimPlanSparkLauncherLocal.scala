package com.intuit.data.simplan.spark.launcher

import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.service.SparkApplication

/** @author Abraham, Thomas - tabraham1
  *         Created on 06-Oct-2021 at 12:58 AM
  */
object SimPlanSparkLauncherLocal extends App {

  private val userConfigs = Array("classpath:spark-dev.conf")

  val context: SparkAppContext = new SparkAppContext(userConfigs)

  SparkApplication(context)
    .addConfigs(
      "classpath:demo/configs/ged-simple-business-logic-sql.conf"
    )
    .run()
  println("\n*************** Done 1 ***************")
}
