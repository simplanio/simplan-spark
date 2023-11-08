package com.intuit.data.simplan.spark.launcher.eventgenerator

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.util

/** @author Abraham, Thomas - tabraham1
  *         Created on 16-Mar-2022 at 7:19 PM
  */
object KafkaProducerDomainEventsApp extends App {

  val props: util.Properties = new util.Properties()
  props.put("bootstrap.servers", "localhost:9092")
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("acks", "all")
  val producer = new KafkaProducer[String, String](props)

  val topic = "st-epop-vendor-source"

  val headerString =
    """{entityId-entityId=133, entityType=com.intuit.smallbusiness.qbo.finance.accounting.accountingautomation.OFXTransaction, eventRecordedTimestamp=2023-02-03T00:09:59.724861Z, entityId-accountId=9130358446058176, entityId=133, schemaURI=com.intuit.smallbusiness.qbo.finance.accounting.accountingautomation.OFXTransaction, tid=drs-9f1b5a40-4aa2-3a40-acf9-a17a9118eb8e, entityVersion=32, b3=7139ef7c53af9cbf-183f0c192ac96a8e-0, nativeHeaders={"b3":["7139ef7c53af9cbf-183f0c192ac96a8e-0"]}, companyId=9130358446058176, singularityheader=notxdetect=true*ctrlguid=1670480129*appId=7002*nodeid=18985441, spring_json_header_types={"nativeHeaders":"org.springframework.util.LinkedMultiValueMap","eventRecordedTimestamp":"java.lang.String","entityChangeAction":"java.lang.String","contentType":"java.lang.String"}, intuitTid=drs-9f1b5a40-4aa2-3a40-acf9-a17a9118eb8e, spring.cloud.stream.sendto.destination=e2e.qbo.finance.accounting.accountingautomation.ofxtransaction.v1, entityChangeAction=CREATE, idempotenceKey=ceab573e-faba-43fc-8f4a-2ac699819f1a, contentType=application/json}"""

  val payloadString =
    """{
      |    "meta": {
      |        "created": "2022-09-30T07:02:09Z",
      |        "createdByUser": {
      |            "id": "2"
      |        },
      |        "updated": "2023-02-03T00:09:54Z",
      |        "updatedByUser": {
      |            "id": "System"
      |        },
      |        "updatedByApp": {
      |            "id": "System"
      |        }
      |    },
      |    "id": {
      |        "entityId": "2",
      |        "accountId": "1"
      |    },
      |    "version": 32,
      |    "sourceEntityVersion": 32,
      |    "active": true,
      |    "domainMetadata": {
      |        "shardId": "23"
      |    },
      |    "payee": "PENNYMAC HOME MORTGAGE",
      |    "type": "DEBIT",
      |    "amount": "-10000.0000000",
      |    "balance": "0.0000000",
      |    "memo": "LOAN PYMT TEST ASSET 8938R837",
      |    "bankAccountId": "5",
      |    "bankSessionId": "1",
      |    "methodId": "0",
      |    "cleanedUpPayee": "Pennymac Home Mortgage",
      |    "category": "Uncategorized Expense:Ask my expert",
      |    "categoryCode": "151",
      |    "categoryId": "151",
      |    "fdpId": "urn:transaction:fdp::transactionid:bff4048b-408d-11ed-a2e2-1eed16a16b16",
      |    "deletedByUser": false,
      |    "date": "2022-07-20",
      |    "currencyType": "USD",
      |    "categorizationConfidenceScore": "0.0000000",
      |    "mintBusinessCategoryCode": "10016",
      |    "merchantId": "0"
      |}""".stripMargin

  //Receipts
//  val payloadString =
//    """{"date":"2022-12-21","amount":"-42.91","itemLines":[],"resolutionType":"ADD","cleansedDescription":"Wal-Mart","fiDescription":"walmart","connectionAccountId":"5e1cb420-7b46-11ed-82fb-3d744f798dc4","accountLines":[{"amount":"42.91","billable":{"billable":false}}],"transactionType":"PURCHASE","processingStatus":"TO_BE_REVIEWED","editSequence":3,"sourceEntity":{"sourceEntityType":"RECEIPT","extendedProperties":[],"sourceSystem":"RECEIPT_UPLOAD"},"shipping":{},"balance":{},"attachment":[{"downloadURI":"https://financialdocument-e2e.platform.intuit.com/v2/documents/2852db47-53aa-44ac-8fce-980348df4f37/sources/1","contentType":"image/jpeg"}],"meta":{"updatedByUser":{"id":"9130358780317736"},"createdByUser":{"id":"9130358780317736"},"created":"2022-12-14T00:57:17.22Z","updatedByApp":{"id":"Intuit.smallbusiness.quickbooksmobile.qbmios"},"createdByApp":{"id":"Intuit.smallbusiness.quickbooksmobile.qbmios"},"updated":"2022-12-14T00:57:27.077Z"},"matchedAccountingTransactions":[],"taxSummary":{"totalTaxableAmount":"0","totalTaxAmount":"0.88","taxType":"EXCLUSIVE","totalTaxInclusiveAmount":"0"},"id":{"accountId":"9130358780317746","entityId":"41548d50-7b4a-11ed-8cb9-c1518b94bff1"},"privateMemo":"","receiptStatus":"PROCESSED"}"""
  try {
    val record1 = new ProducerRecord[String, String](topic, payloadString)
    val headers = headerString
      .replace("{", "")
      .replace("}", "")
      .split(",")
      .map(_.trim)
      .map(each => {
        val s = each.split("=")
        (s.head, s(s.size - 1))
      })

    headers.foreach(each => record1.headers().add(each._1, each._2.getBytes))
    producer.send(record1)
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }

}
