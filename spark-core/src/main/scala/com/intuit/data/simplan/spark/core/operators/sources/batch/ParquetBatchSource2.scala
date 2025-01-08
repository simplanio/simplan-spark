/*
 *  Copyright 2024, Intuit Inc
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
package com.intuit.data.simplan.spark.core.operators.sources.batch

import com.intuit.data.simplan.core.domain.{Lineage, TableType}
import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.core.domain.operator.config.transformations.SqlStatementConfig
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.sources.SparkBatchSourceConfig
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse}
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import com.intuit.data.simplan.spark.core.operators.sources.BatchSourceUtil
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 07-Nov-2024 at 11:39 PM
  */

class ParquetBatchSource2(appContext: SparkAppContext, operatorContext: OperatorContext) extends SparkOperator[SparkBatchSourceConfig](appContext, operatorContext) with Logging {

  lazy val util = new BatchSourceUtil(appContext, operatorContext, operatorConfig.copy(format = "parquet"))

  override def getJobDescription: String = super.getJobDescription + s"\nPath : ${util.path}"

  override def process(request: SparkOperatorRequest): SparkOperatorResponse = util.process(request)
}
