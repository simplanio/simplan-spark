/*
*  Copyright 2025, Intuit Inc
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*         http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*/
package com.intuit.data.simplan.spark.core.testutils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author Abraham, Thomas - tabraham1
 *         Created on 07-Mar-2025 at 10:26 AM
 */
object SparkTestingUtils {

  val spark: SparkSession = SparkSession.builder()
    .appName("TestUtils")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  def getSampleDataFrameWithData: DataFrame = {
    val sourceData2: Seq[UsersModel] = Seq(
      UsersModel("Abraham", 25, "Fremont"),
      UsersModel("Thomas", 35, "Sunnyvale"),
    )
    sourceData2.toDF
  }
}