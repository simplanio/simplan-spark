package com.intuit.data.simplan.spark.core.utils

import com.intuit.data.simplan.core.domain.TableType
import com.intuit.data.simplan.core.util.PartitionOptimizerResult
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.domain.operator.config.SparkRepartitionConfig
import org.apache.spark.sql.functions.{col, lit, udf}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import scala.annotation.tailrec
import scala.util.Try

/** @author Abraham, Thomas - tabraham1
 *          Created on 26-Apr-2022 at 11:03 PM
 */
object DataframeUtils extends Logging {
  def createOrReplaceTempView(dataframe: DataFrame, tableType: TableType, name: String) = if (tableType == TableType.TEMP) dataframe.createOrReplaceTempView(name)

  def getLocationOfTable(spark: SparkSession, tableName: String): Option[String] = Try(spark.sql(s"describe formatted $tableName")
    .toDF //convert to dataframe will have 3 columns col_name,data_type,comment
    .filter(col("col_name") === "Location") //filter on column name
    .collect()(0)(1)
    .toString).toOption

  def repartition(spark: SparkSession, dataframe: DataFrame, repartitionConfig: SparkRepartitionConfig, dataframeName: String = ""): (DataFrame, Option[PartitionOptimizerResult]) = {
    val optimisedPartitions: Option[PartitionOptimizerResult] = repartitionConfig.calculateOptimisedPartition(spark)

    if (optimisedPartitions.isEmpty) {
      logger.info(s"Skipping Repartitioning dataframe ${dataframe.toString()}. Unable to calculate Optimised Partition")
      return (dataframe, optimisedPartitions)
    }

    if (optimisedPartitions.get.proposedPartitionCount.isEmpty) {
      logger.info(s"Skipping Repartitioning dataframe ${dataframe.toString()}. No Proposed Partition Count is determined")
      return (dataframe, optimisedPartitions)
    }

    if (repartitionConfig.resolvedColumns.nonEmpty) {
      logger.info(s"Repartitioning dataframe($dataframeName) with ${optimisedPartitions.get.proposedPartitionCount.get} partitions on columns ${repartitionConfig.resolvedColumns.mkString(",")} ")
      (
        dataframe.repartition(optimisedPartitions.get.proposedPartitionCount.get, repartitionConfig.resolvedColumns.map(col): _*),
        optimisedPartitions
      )
    } else {
      logger.info(s"Repartitioning dataframe($dataframeName) with ${optimisedPartitions.get.proposedPartitionCount.get} partitions")
      (
        dataframe.repartition(optimisedPartitions.get.proposedPartitionCount.get),
        optimisedPartitions
      )
    }
  }


  def isFieldExisting(df: DataFrame, fieldPath: String): Boolean = {
    val path = fieldPath.toLowerCase.split("\\.").toList
    val bool = isFieldExisting(df.schema, path)
    bool
  }

  @tailrec
  private def isFieldExisting(struct: StructType, path: List[String]): Boolean = {
    path.length match {
      case 0 => false // should not happen
      case 1 => struct.fieldNames.map(_.toLowerCase).contains(keyCleanser(path.head)) // last element
      case _ => struct.fields.find(each => each.name.toLowerCase == keyCleanser(path.head)) match {
        case Some(StructField(_, nestedStruct: StructType, _, _)) => isFieldExisting(nestedStruct, path.tail)
        case _ => false
      }
    }
  }

  def findFieldIfExisting(df: DataFrame, fieldPath: String): Option[StructField] = {
    val path = fieldPath.toLowerCase.split("\\.").toList
    val bool = findFieldIfExisting(df.schema, path)
    bool
  }

  @tailrec
  private def findFieldIfExisting(struct: StructType, path: List[String]): Option[StructField] = {
    path.length match {
      case 0 => None // should not happen
      case 1 => struct.find(each => each.name.equalsIgnoreCase(keyCleanser(path.head)))
      case _ => struct.fields.find(each => each.name.toLowerCase == keyCleanser(path.head)) match {
        case Some(StructField(_, nestedStruct: StructType, _, _)) => findFieldIfExisting(nestedStruct, path.tail)
        case _ => None
      }
    }
  }

  def getPartitionColumns(tableName: String)(implicit spark: SparkSession): Array[String] = {
    val describeTableQuery = s"DESCRIBE FORMATTED $tableName"
    logger.info(s"Fetching partition columns for table $tableName using query : $describeTableQuery")
    val describeResult = spark.sql(describeTableQuery).collect()
    // Extract the partition columns
    Try(describeResult
      .dropWhile(row => !row.getString(0).contains("# Partition Information"))
      .drop(1)
      .takeWhile(row => row.getString(0).nonEmpty)
      .map(row => row.getString(0))
      .filter(each => each.nonEmpty && !each.contains(" ") && !each.contains("#"))).getOrElse(Array.empty)
  }

  val getPartitionColumnValue: (String, String) => String = (partitionString, partitionCol) => if (partitionString != null) partitionString.split("/").filter(_.startsWith(partitionCol)).map(_.split("=")(1)).head else null
  val getPartitionColumnValueUdf = udf(getPartitionColumnValue)

  def getPartitionInformation(tableName: String, partitionSpec: Map[String, String] = Map.empty)(implicit spark: SparkSession): Try[DataFrame] = {
    Try {
      logger.info(s"Fetching partition information for table $tableName with partitionSpec ${partitionSpec.mkString(",")}")
      val partitionColumns = getPartitionColumns(tableName)
      logger.info(s"Partition columns for table $tableName are ${partitionColumns.mkString(",")}")
      val partitionSpecSqlSection = partitionColumns match {
        case _ if partitionColumns nonEmpty =>
          val partitionSpecString = partitionSpec.map { case (key, value) => s"$key='$value'" }.mkString(", ")
          "PARTITION (" + partitionSpecString + ")"
        case _ => ""
      }
      val showPartitionQuery = s"""SHOW PARTITIONS $tableName $partitionSpecSqlSection"""
      logger.info(s"Fetching partition information for table $tableName using query : $showPartitionQuery")
      val partitionDf = spark.sql(showPartitionQuery)
      partitionColumns.foldLeft(partitionDf)((df, each) => df.withColumn(each, getPartitionColumnValueUdf(col("partition"), lit(each))))
    }
  }

  private def keyCleanser(keyName: String): String = keyName.toLowerCase

  def count(dataframe: DataFrame, uniqueColumns: List[String] = List.empty): Long =
    if (uniqueColumns.isEmpty) dataframe.count()
    else dataframe.dropDuplicates(uniqueColumns).count()

  def printSchema(heading: String, dataframe: DataFrame): Unit =
    if (logger.isDebugEnabled()) {
      println(s"===== Start : $heading ======")
      dataframe.printSchema()
      println(s"===== End : $heading ======")
    }

  def rowToMap(row: Row): java.util.Map[String, AnyRef] = {
    val map = new java.util.HashMap[String, AnyRef]()
    row.schema.fieldNames.foreach(each => map.put(each, row.getAs(each).asInstanceOf[AnyRef]))
    map
  }
}
