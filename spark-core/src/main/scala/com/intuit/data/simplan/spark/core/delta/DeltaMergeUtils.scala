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

package com.intuit.data.simplan.spark.core.delta

import com.intuit.data.simplan.spark.core.domain.operator.config.transformations.{Condition, DeltaMergeConfig}
import io.delta.tables.{DeltaMergeBuilder, DeltaTable}
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 04-Sep-2022 at 10:25 AM
  */
object DeltaMergeUtils {

  def createDeltaMergeBuilder(deltaTable: DeltaTable, deltaEvents: DataFrame, mergeConfig: DeltaMergeConfig): DeltaMergeBuilder = {
    // TODO: When Delta adds updateAllExcept and insertAllExcept method, switch over to these methods to
    // exclude the entityChangeAction column
    val deltaMergeBuilder: DeltaMergeBuilder =
      deltaTable.as(mergeConfig.deltaTableSourceAlias).merge(deltaEvents.alias(mergeConfig.deltaEventsSourceAlias), mergeConfig.mergeCondition)

    val matchConditions: Seq[Condition] = mergeConfig.matchCondition
    val notMatchConditions: Seq[Condition] = mergeConfig.notMatchCondition

    val matchDeltaMergeBuilder =
      if (matchConditions.isEmpty) deltaMergeBuilder.whenMatched().updateAll()
      else
        matchConditions.foldLeft(deltaMergeBuilder) { (z, f) =>
          {
            f.action match {
              case "UPDATE" => z.whenMatched(f.expression).updateAll()
              case "DELETE" => z.whenMatched(f.expression).delete()
              case _        => throw new Exception("UnSupported Type")
            }
          }
        }

    val mergeBuilder: DeltaMergeBuilder =
      if (notMatchConditions.isEmpty) matchDeltaMergeBuilder.whenNotMatched().insertAll()
      else
        notMatchConditions.foldLeft(matchDeltaMergeBuilder) { (z, f) =>
          {
            f.action match {
              case "INSERT" => z.whenNotMatched(f.expression).insertAll()
              case _        => throw new Exception("UnSupported Type")
            }
          }
        }
    mergeBuilder
  }

  def forEachBatchDataframeWriter(formattedEvents: DataFrame, batchId: Long)(implicit deltaTable: DeltaTable, mergeOperatorConfig: DeltaMergeConfig): Unit = {
    val builder = DeltaMergeUtils.createDeltaMergeBuilder(deltaTable, formattedEvents, mergeOperatorConfig)
    builder.execute()
  }

}
