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

package com.intuit.data.simplan.spark.core.operators

import com.intuit.data.simplan.common.exceptions.SimplanConfigException
import com.intuit.data.simplan.spark.core.suite.SimplanSparkJobTestFunSuite
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import scala.util.Try

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 03-Oct-2023 at 9:57 AM
  */
class BasicSanityChecks extends SimplanSparkJobTestFunSuite {

  test("Read Json And Simple Perform SQL Operation") { simplanJob =>
    val configFile = "classpath:jobs/sanity-checks/ReadJsonAndPerformSQLOperation.conf"

    simplanJob
      .withConfigs(configFile)
      .will { (spark, _) =>
        spark.sql("select * from SqlStepToJoinAndFilter").count() shouldBe 4
      }
  }

  test("Should throw an exception when invalid operator is provided") { simplanJob =>
    val configFile = "classpath:jobs/sanity-checks/InvalidOperator.conf"

    simplanJob
      .withConfigs(configFile)
      .willFailWith[SimplanConfigException]
  }
}
