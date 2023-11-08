package com.intuit.data.simplan.spark.core.monitoring

import com.intuit.data.simplan.core.context.AppContext
import org.apache.spark.sql.streaming.{SourceProgress, StreamingQueryProgress}

/** @author Abraham, Thomas - tabraham1
  *         Created on 27-Apr-2022 at 3:14 PM
  */
case class StreamMonitoringEvent(appName: String, appRunId: String, streamName: String, streamRunId: String, streamId: String, timestamp: String, streamDesc: String, startOffset: String, endOffset: String, inputRows: Long, inputRowsPerSecond: Double, processedRowsPerSecond: Double)

object StreamMonitoringEvent {

  def apply(appContext: AppContext, progress: StreamingQueryProgress, source: SourceProgress): StreamMonitoringEvent =
    StreamMonitoringEvent(
      appName = appContext.appContextConfig.application.name,
      appRunId = appContext.appContextConfig.application.runId.get,
      streamName = progress.name,
      streamRunId = progress.runId.toString,
      streamId = progress.id.toString,
      timestamp = progress.timestamp,
      streamDesc = source.description,
      startOffset = source.startOffset,
      endOffset = source.endOffset,
      inputRows = source.numInputRows,
      inputRowsPerSecond = source.inputRowsPerSecond,
      processedRowsPerSecond = source.processedRowsPerSecond
    )
}
