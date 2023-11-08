///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to you under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.intuit.data.simplan.spark.core.operators.transformations.dynamodb
//
//import com.amazonaws.services.dynamodbv2.model.AttributeValue
//import com.intuit.data.simplan.core.supports.AmazonDynamoSupport
//import org.apache.spark.sql.types._
//import org.apache.spark.sql.{ForeachWriter, Row}
//
//import scala.collection.JavaConverters._
//
//class DynamoWriter(support: AmazonDynamoSupport, tableName: String) extends ForeachWriter[Row] {
//
//  lazy val dynamoDbClient = support.dynamonDbClient
//
//  def open(partitionId: Long, epochId: Long) = {
//    dynamoDbClient
//    true
//  }
//
//  def process(row: Row) = {
//
//    val rowAsMap = row.getValuesMap(row.schema.fieldNames)
//
//    val dynamoItemS: Map[String, AttributeValue] = rowAsMap.mapValues {
//      v: Any => new AttributeValue(v.toString)
//    }
//
//    val dynamoItem = rowAsMap.mapValues {
//      v: Any => new AttributeValue(v.toString)
//    }.asJava
//
//    val fields: Array[StructField] = row.schema.fields
//
//    val dynamoItemS3: Map[String, AttributeValue] = fields.map(field => {
//      field.dataType match {
//        case LongType => {
//          val attr = new AttributeValue()
//          val index = row.fieldIndex(field.name)
//          val value = row.getLong(index)
//          val n: AttributeValue = attr.withN(value.toString)
//          (field.name -> n)
//        }
//        case StringType => {
//          val attr = new AttributeValue()
//          val index = row.fieldIndex(field.name)
//          val value = row.getString(index)
//          val s: AttributeValue = attr.withS(value.toString)
//          (field.name -> s)
//        }
//        case TimestampType => {
//          val attr = new AttributeValue()
//          val index = row.fieldIndex(field.name)
//          val value = row.getTimestamp(index)
//          val s: AttributeValue = attr.withS(value.toString)
//          (field.name -> s)
//        }
//        case BooleanType => {
//          val attr = new AttributeValue()
//          val index = row.fieldIndex(field.name)
//          val value = row.getBoolean(index)
//          val s: AttributeValue = attr.withS(value.toString)
//          (field.name -> s)
//        }
//        case _ => {
//          val attr = new AttributeValue()
//          val value = row.getAs(field.name)
//          val s: AttributeValue = attr.withS(value.toString)
//          (field.name -> s)
//        }
//      }
//    }).toMap
//
//    dynamoDbClient.putItem(tableName, dynamoItemS3.asJava)
//  }
//
//  def close(errorOrNull: Throwable) = {
//    dynamoDbClient.shutdown()
//  }
//
//}
