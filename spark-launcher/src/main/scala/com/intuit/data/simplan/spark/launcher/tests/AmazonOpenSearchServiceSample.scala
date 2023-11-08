//package com.intuit.data.simplan.spark.launcher.tests
//
//import com.amazonaws.auth.{AWS4Signer, DefaultAWSCredentialsProviderChain}
//import com.amazonaws.http.AWSRequestSigningApacheInterceptor
//import org.apache.http.HttpHost
//import org.opensearch.action.index.IndexRequest
//import org.opensearch.client.{RequestOptions, RestClient, RestHighLevelClient}
//
//import java.io.IOException
//import java.util
//
///**
//  * @author Abraham, Thomas - tabraham1
//  *         Created on 06-May-2022 at 5:12 PM
//  */
//object AmazonOpenSearchServiceSample {
//
//  private val serviceName = "es"
//  private val region = "us-west-2" // e.g. us-east-1
//
//  private val host = "https://simplan-opensearch.data-curation-prd.a.intuit.com" // e.g. https://search-mydomain.us-west-1.es.amazonaws.com
//
//  private val index = "my-index"
//  private val `type` = "_doc"
//  private val id = "1"
//
//  @throws[IOException]
//  def main(args: Array[String]): Unit = {
//    val searchClient = searchClient1(serviceName, region)
//    // Create the document as a hash map
//    val document = new util.HashMap[String, Object]()
//    document.put("title", "Walk the Line")
//    document.put("director", "James Mangold")
//    document.put("year", "2005")
//    // Form the indexing request, send it, and print the response
//    val request = new IndexRequest(index, `type`, id).source(document)
//    val response = searchClient.index(request, RequestOptions.DEFAULT)
//    System.out.println(response.toString)
//    println("hello")
//  }
//
//  // Adds the interceptor to the OpenSearch REST client
//  def searchClient1(serviceName: String, region: String): RestHighLevelClient = {
//    val signer = new AWS4Signer
//    signer.setServiceName(serviceName)
//    signer.setRegionName(region)
//    val interceptor = new AWSRequestSigningApacheInterceptor(serviceName, signer, credentialsProvider)
//    new RestHighLevelClient(RestClient.builder(HttpHost.create(host)).setHttpClientConfigCallback(hacb => hacb.addInterceptorLast(interceptor)))
//  }
//}
