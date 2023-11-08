package com.intuit.data.simplan.spark.core.monitoring

import com.intuit.data.simplan.spark.core.context.SparkAppContext

/** @author Abraham, Thomas - tabraham1
  *         Created on 27-Apr-2022 at 1:53 PM
  */
class StreamMonitoring(appContext: SparkAppContext) {
  def attachMonitoringListener(): Unit = appContext.spark.streams.addListener(new SimplanStreamingQueryListener(appContext))

}

object StreamMonitoring {
  def apply(appContext: SparkAppContext): StreamMonitoring = new StreamMonitoring(appContext)
}
