package com.intuit.data.simplan.spark.launcher.eventgenerator

import java.time.Instant
import java.util.UUID

case class Context(
    ecs_version: String = "4.0",
    auth_id: Option[String] = None,
    ivid: String = "9a23b17f-e731-471b-9250-9b2b63ca596e",
    timezone: String = "America/Los_Angeles",
    page: Page = Page(),
    locale: String = "en-US"
) extends Serializable

case class Page(
    url: String = "https://signup.quickbooks.intuit.com/?locale=en-us&offerType=trial&bc=OBI-LL1&offerId=20020222",
    path: String = "/",
    referrer: String = "https://quickbooks.intuit.com/"
) extends Serializable

case class Properties(
    org: String = "sbseg",
    os: String = "MacIntel",
    page_path: String = "/app/homepage",
    page_title: String = "QuickBooks",
    page_language: String = "en",
    platform: String = "web",
    scope: String = "qbo",
    sku: String = "SS_2010",
    url_host_name: String = "app.qbo.intuit.com"
) extends Serializable

case class ECSEvent(
    timestamp: Instant = Instant.now(),
    sorter: Long = 0,
    context: Context = Context(),
    properties: Properties = Properties(),
    messageId: String = UUID.randomUUID().toString.replaceAll("-", "")
) extends Serializable

object ECSEvent extends Serializable {

  def apply(visitor: IVisitor): ECSEvent = {
    val context = Context().copy(
      ivid = visitor.iVid,
      timezone = visitor.timezone,
      locale = visitor.locale
    )
    val properties = Properties().copy(
      org = visitor.org,
      os = visitor.os,
      platform = visitor.platform
    )

    ECSEvent().copy(
      context = context,
      properties = properties
    )
  }

}
