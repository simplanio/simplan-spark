/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intuit.data.simplan.spark.launcher.tests

import org.apache.spark.sql.SparkSession

import java.time.Instant
import scala.util.Random

/** @author Abraham, Thomas - tabraham1
  *         Created on 30-Jan-2023 at 1:43 PM
  */

case class Data(id: BigInt, firstName: String, lastName: String, location: String, workingOn: String, lastUpdated: Instant = Instant.now().minusSeconds(Random.nextInt(30) * 30))

object DataGenerator {

  val data = List(
    Data(1, "Thomas", "Abraham", "Sunnyvale", "Simplan"),
    Data(2, "Kiran", "Hiremath", "Irvine", "Simplan"),
    Data(3, "Rama", "Arvabhumi", "Milpitas", "Simplan"),
    Data(4, "Shradha", "Ambekar", "Mountain View", "Simplan"),
    Data(5, "Anand", "Elluru", "North Carolina", "Superglue"),
    Data(6, "Sooji", "Son", "Ohio", "Superglue"),
    Data(7, "Yang", "Zhou", "San Jose", "Superglue"),
    Data(8, "Varun", "Sood", "San Jose", "Superglue")
  )

  val spark = SparkSession.builder()
    .appName("Integrating Kafka")
    .master("local[2]")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(data).toDF()
    df.write.format("parquet").save("/Users/tabraham1/Intuit/Temp/Parquet")
  }
}
