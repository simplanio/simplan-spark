package com.intuit.data.simplan.spark.core.operators.transformations

import com.intuit.data.simplan.common.exceptions.SimplanConfigException
import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.{SessionWindowOperatorConfig, SlidingWindowOperatorConfig, TumbleWindowOperatorConfig, WindowConfig}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import org.apache.spark.sql.functions._

class WindowOperator(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[WindowConfig](appContext, operatorContext) {

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = {
    val frame = request.dataframes(operatorConfig.source)

    val windowFrame = operatorConfig.window match {
      case tumble: TumbleWindowOperatorConfig   => frame.withColumn("window", window(column(tumble.timeColumn), tumble.windowDuration))
      case slide: SlidingWindowOperatorConfig   => frame.withColumn("window", window(column(slide.timeColumn), slide.windowDuration, slide.slideDuration))
      case session: SessionWindowOperatorConfig => frame.withColumn("window", window(column(session.timeColumn), session.windowDuration, session.slideDuration, session.startTime))
      case _                                    => throw new SimplanConfigException("Invalid window operator config")
    }
    new SparkOperatorResponse(true, Map(operatorContext.taskName -> windowFrame))
  }

}
