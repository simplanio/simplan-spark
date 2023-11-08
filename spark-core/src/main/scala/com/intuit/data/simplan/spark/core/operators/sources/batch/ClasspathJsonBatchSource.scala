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

package com.intuit.data.simplan.spark.core.operators.sources.batch

import com.intuit.data.simplan.core.domain.operator.OperatorContext
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.operators.sources.AbstractBatchSource
import org.apache.spark.sql.DataFrame

import java.io.InputStream

/**
  * @author Abraham, Thomas - tabraham1
  *         Created on 03-Oct-2023 at 9:43 AM
  */
class ClasspathJsonBatchSource(sparkAppContext: SparkAppContext, operatorContext: OperatorContext) extends AbstractBatchSource(sparkAppContext, operatorContext){
  override def loadBatchData: DataFrame = {
    val allOptions = operatorConfig.resolvedOptions
    val schema = getSchemaFromJson(operatorConfig.schema)
    logger.info(s"Resolved Path to load for ${operatorContext.taskName} : $path")

    val stream: InputStream = getClass.getResourceAsStream(path)
    val content = scala.io.Source.fromInputStream(stream)
      .getLines
      .toList
    import sparkAppContext.spark.implicits._
    val contentAsRDD = sparkAppContext.spark.sparkContext.parallelize(content).toDS

    val readWithSchema = if (schema.isDefined) sparkAppContext.spark.read.schema(schema.get) else sparkAppContext.spark.read
    allOptions match {
      case options if options.nonEmpty => readWithSchema.options(options).json(contentAsRDD)
      case _ => readWithSchema.json(contentAsRDD)
    }
  }


}
