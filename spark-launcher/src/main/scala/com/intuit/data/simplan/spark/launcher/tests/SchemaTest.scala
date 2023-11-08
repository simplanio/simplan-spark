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

import org.apache.spark.sql.types._

/** @author Abraham, Thomas - tabraham1
  *         Created on 03-Feb-2023 at 10:05 AM
  */
object SchemaTest {

  def schemaTest = {
    val schema = StructType(Array(
      StructField("firstname", StringType, true),
      StructField("middlename", StringType, true),
      StructField("lastname", StringType, true),
      StructField(
        "id",
        StructType(Array(
          StructField("firstname", StringType, true),
          StructField("middlename", StringType, true),
          StructField("lastname", StringType, true),
          StructField("id", StringType, true),
          StructField("gender", StringType, true),
          StructField("salary", IntegerType, true)
        )),
        true
      ),
      StructField(
        "gender",
        ArrayType(StructType(Array(
          StructField("firstname", StringType, true),
          StructField("middlename", StringType, true),
          StructField("lastname", StringType, true),
          StructField("id", StringType, true),
          StructField("gender", StringType, true),
          StructField("salary", IntegerType, true)
        ))),
        true
      ),
      StructField(
        "gender1",
        ArrayType(IntegerType, true),
        true
      ),
      StructField("labels", MapType(StringType, StringType), true),
      StructField("Salary", DecimalType(38,2), true)
    ))
    println("============")
    println(schema.json)
    println("============")
    schema.printTreeString()
    println("============")
  }

  def main(args: Array[String]): Unit = {
    schemaTest
  }
}
