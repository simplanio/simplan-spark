package com.intuit.data.simplan.spark.launcher.eventgenerator

import com.intuit.data.simplan.global.utils.SimplanImplicits._
import org.apache.commons.io.FileUtils

import java.io.File

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 03-May-2022 at 5:11 PM
  */
object ClickstreamEventsGenerator {

  val MAX_PRE_LOGIN_EVENTS = 2
  val MAX_LOGGEDIN_EVENTS = 5
  val MAX_LOGOUT_EVENTS = 3

  def generateEvents(numOfUsers: Int) = (1 to numOfUsers).map(_ => VisitorEvents.generate(IVisitor()).sortBy(_.sorter)).toList.flatten.sortBy(_.sorter)

  def main(args: Array[String]): Unit = {
    val FILE_LOCATION = "/Users/tabraham1/Intuit/Temp/EventGenerated"
    val events: List[ECSEvent] = generateEvents(1)
    val str: String = events.map(_.toJson).mkString("\n")
    FileUtils.writeStringToFile(new File(FILE_LOCATION + "/" + System.currentTimeMillis() + ".json"), str, "UTF-8")
  }
}
