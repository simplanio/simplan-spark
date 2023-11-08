package com.intuit.data.simplan.spark.launcher.console

import com.intuit.data.simplan.core.context.console.{ConsoleAppContext, ConsoleApplication}
import com.intuit.data.simplan.core.context.{InitContext, DefaultRunParameters}

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 06-Oct-2021 at 12:59 AM
  */
object ConsoleLauncher extends App {

  val context = new ConsoleAppContext(
    initContext = InitContext(userConfigs = Array("/Users/tabraham1/Intuit/Development/GitHub/Enterprise/tabraham1/SimPlan/spark-launcher/src/main/resources/configs/console-sample.conf")))
  ConsoleApplication(context).run(new DefaultRunParameters())

}
