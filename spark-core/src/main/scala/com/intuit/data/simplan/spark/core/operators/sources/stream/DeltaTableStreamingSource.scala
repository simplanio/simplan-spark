package com.intuit.data.simplan.spark.core.operators.sources.stream

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.operators.sources.AbstractStreamingSource

/** @author Abraham, Thomas - tabraham1
  *         Created on 08-Dec-2021 at 2:14 PM
  */

class DeltaTableStreamingSource(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractStreamingSource(sparkAppContext, operatorContext, "delta")
