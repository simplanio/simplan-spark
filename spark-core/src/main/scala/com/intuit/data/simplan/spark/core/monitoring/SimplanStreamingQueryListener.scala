package com.intuit.data.simplan.spark.core.monitoring

import com.intuit.data.simplan.global.utils.SimplanImplicits._
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import org.apache.spark.sql.streaming.StreamingQueryListener.{QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryListener, StreamingQueryProgress}
import org.slf4j.LoggerFactory

/** @author Abraham, Thomas - tabraham1
  *         Created on 27-Apr-2022 at 3:25 PM
  */
class SimplanStreamingQueryListener(sparkAppContext: SparkAppContext) extends StreamingQueryListener {
  private lazy val streamMonitoringLogger = LoggerFactory.getLogger("simplanStreamMonitor")
  private lazy val logger = LoggerFactory.getLogger(classOf[SimplanStreamingQueryListener])

  override def onQueryStarted(queryStarted: QueryStartedEvent): Unit = logger.info(s"Query started: ${queryStarted.id} Name : ${queryStarted.name}")

  override def onQueryTerminated(queryTerminated: QueryTerminatedEvent): Unit = logger.info(s"Query Terminated: ${queryTerminated.id} Exception : ${queryTerminated.exception.getOrElse("None")}")

  override def onQueryProgress(queryProgress: QueryProgressEvent): Unit = {
    val progress: StreamingQueryProgress = queryProgress.progress
    progress.sources.foreach((source: SourceProgress) => streamMonitoringLogger.info(StreamMonitoringEvent(sparkAppContext, progress, source).toJson))
  }
}
