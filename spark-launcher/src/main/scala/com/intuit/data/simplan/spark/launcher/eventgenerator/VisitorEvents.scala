package com.intuit.data.simplan.spark.launcher.eventgenerator

import com.intuit.data.simplan.spark.launcher.eventgenerator.ClickstreamEventsGenerator.{MAX_LOGGEDIN_EVENTS, MAX_LOGOUT_EVENTS, MAX_PRE_LOGIN_EVENTS}

import java.text.SimpleDateFormat
import java.util.Date
import scala.util.{Random, Try}

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 03-May-2022 at 4:59 PM
  */
object VisitorEvents extends Serializable {
  val pattern = "yyddss"
  val simpleDateFormat = new SimpleDateFormat(pattern)

  val prefixDate: String = simpleDateFormat.format(new Date())

  def generate(visitor: IVisitor): List[ECSEvent] = {
    val events = scala.collection.mutable.ListBuffer[ECSEvent]()
    val eventTemplate = ECSEvent(visitor)
    println(s"Is Login for ${visitor.iVid} = ${visitor.isLogin}")
    if (visitor.isLogin) {
      events += eventTemplate.copy(sorter = (prefixDate + visitor.loginTime).toLong, properties = eventTemplate.properties.copy(page_path = "app/login"))
      events += eventTemplate.copy(sorter = (prefixDate + visitor.logoutTime).toLong, properties = eventTemplate.properties.copy(page_path = "app/logout"))
      for (_ <- 0 to 1 + random(MAX_PRE_LOGIN_EVENTS).toInt) {
        events += eventTemplate.copy(sorter = (prefixDate + random(visitor.loginTime)).toLong, properties = eventTemplate.properties.copy(page_path = "/app/homepage"))
      }
      for (_ <- 0 to 1 + random(MAX_LOGGEDIN_EVENTS).toInt) {
        events += eventTemplate.copy(
          sorter = (prefixDate + randomBetween(visitor.loginTime, visitor.logoutTime)).toLong,
          context = eventTemplate.context.copy(auth_id = visitor.auth_id),
          properties = eventTemplate.properties.copy(page_path = visitor.page_path)
        )
      }
      for (_ <- 0 to 1 + random(MAX_LOGOUT_EVENTS).toInt) {
        events += eventTemplate.copy(
          sorter = (prefixDate + randomBetween(visitor.loginTime + visitor.logoutTime, visitor.loginTime)).toLong,
          properties = eventTemplate.properties.copy(page_path = "/app/homepage")
        )
      }
    } else {
      for (_ <- 1 to random(30).toInt) {
        events += eventTemplate.copy(
          sorter = (prefixDate + random(visitor.logoutTime)).toLong,
          properties = eventTemplate.properties.copy(page_path = getRandomElement(List("/app/homepage", "/app/contact", "/app/aboutus")))
        )
      }
    }
    events.toList
  }
  def getRandomElement(seq: Seq[String]): String = seq(Random.nextInt(seq.length))

  def randomBetween(from: Long, to: Long) = {
    Try(Thread.sleep(1))
    from + System.nanoTime() % (1 + to)
  }

  def random(bound: Long): Long = {
    Try(Thread.sleep(1))
    System.nanoTime() % (bound + 1)
  }
}
