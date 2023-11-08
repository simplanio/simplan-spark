package com.intuit.data.simplan.spark.launcher.eventgenerator

import com.intuit.data.simplan.spark.launcher.eventgenerator.VisitorEvents.{getRandomElement, random}

import java.util.UUID
import scala.util.Random

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 03-May-2022 at 5:29 PM
  */
case class IVisitor(
    iVid: String = UUID.randomUUID().toString.replace("-", ""),
    auth_id: Option[String] = Some(System.nanoTime().toString + Random.nextLong().toString)
) extends Serializable {
  private val orgLists: List[String] = List("sbseg", "cognition", "cg")
  private val osList: List[String] = List("MacIntel", "AppleSilicon", "Windows10", "LinuxFedora")
  private val platformList: List[String] = List("web", "android", "ios")
  private val timezones: List[String] = List("America/Los_Angeles", "America/New_York", "America/Chicago")
  val pagePaths = List("/app/homepage", "/app/subscription", "/app/addAccount")

  val isLogin: Boolean = System.nanoTime() % 7 != 0
  val org: String = getRandomElement(orgLists)
  val os: String = getRandomElement(osList)
  val platform: String = getRandomElement(platformList)
  val timezone: String = getRandomElement(timezones)
  val locale: String = getRandomElement(List("en-US"))

  def page_path: String = {
    getRandomElement(pagePaths)
  }
  val loginTime = random(1000)
  val logoutTime = loginTime + random(100000)

}
