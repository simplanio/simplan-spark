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

package com.intuit.data.simplan.spark.core.suite

import com.intuit.data.simplan.spark.core.context.SparkAppContext
import org.scalatest.funsuite.AnyFunSuiteLike
import org.scalatest.{BeforeAndAfterAll, Tag}

import java.io.File
import scala.util.Try

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 28-Sep-2023 at 9:03 AM
  */
abstract class SimplanSparkJobTestFunSuite(appContextConfigs: List[String] = List.empty) extends AnyFunSuiteLike with BeforeAndAfterAll {

  lazy val context: SparkAppContext = SparkAppContext((appContextConfigs ++ Array("classpath:spark-test.conf")).toArray)

  def test(name: String)(f: SimplanSparkJobTestBuilder => Unit): Unit = {
    super.test(name) {
      performTest(f)
    }
  }

  private def performTest(f: SimplanSparkJobTestBuilder => Unit): Unit = {
    val testBuilder = new SimplanSparkJobTestBuilder(context)
    f(testBuilder)
  }

  def test(name: String, testTags: Tag*)(f: SimplanSparkJobTestBuilder => Unit): Unit = {
    super.test(name, testTags: _*) {
      performTest(f)
    }
  }

  override def afterAll() {
    Try {
      context.sc.stop()
      context.spark.stop()
    }
    val warehouse = new File("spark-warehouse")
    if (warehouse.exists()) warehouse.delete()
    val derby = new File("derby.log")
    if (derby.exists()) derby.delete()
  }

}
