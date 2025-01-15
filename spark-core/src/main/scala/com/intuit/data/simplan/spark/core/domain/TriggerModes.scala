package com.intuit.data.simplan.spark.core.domain

import com.intuit.data.simplan.common.exceptions.SimplanConfigException
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.streaming.Trigger.{Continuous, Once, ProcessingTime}

/** @author Kiran Hiremath
 */
private[core] object TriggerModes {

  /** Internal helper method to generate objects representing various TriggerMode
   *
   * @param triggerMode modes could be microBatch, continuous, once.
   * @param intervalMs  interval in milliseconds
   * @return Trigger
   */
  def apply(triggerMode: String, intervalMs: Long): Trigger = {
    triggerMode.toUpperCase() match {
      case "PROCESSINGTIME" => ProcessingTime(intervalMs)
      case "CONTINUOUS" => Continuous(intervalMs)
      case "ONCE" => Once()
      case "AVAILABLENOW" => Trigger.AvailableNow()
      case _ => throw new SimplanConfigException(s"Unknown Trigger mode $triggerMode. Accepted output modes are 'ProcessingTime', 'Continuous', 'Once'")
    }
  }
}
