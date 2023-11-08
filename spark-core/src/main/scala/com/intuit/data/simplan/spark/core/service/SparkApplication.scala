package com.intuit.data.simplan.spark.core.service

import com.intuit.data.simplan.core.context.Application
import com.intuit.data.simplan.spark.core.context.SparkAppContext

import scala.util.Try

/** @author - Abraham, Thomas - tabaraham1
  *         Created on 8/19/21 at 11:12 PM
  */
class SparkApplication(sparkAppContext: SparkAppContext) extends Application(sparkAppContext) {
  override def stop: Boolean = Try(sparkAppContext.spark.stop()).isSuccess
}

object SparkApplication {
  def apply(appContext: SparkAppContext): SparkApplication = new SparkApplication(appContext)
}
