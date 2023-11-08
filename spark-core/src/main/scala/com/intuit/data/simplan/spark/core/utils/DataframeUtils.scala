package com.intuit.data.simplan.spark.core.utils

import com.intuit.data.simplan.core.domain.TableType
import com.intuit.data.simplan.core.util.PartitionOptimizerResult
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.domain.operator.config.SparkRepartitionConfig
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.Try

/** @author Abraham, Thomas - tabraham1
  *         Created on 26-Apr-2022 at 11:03 PM
  */
object DataframeUtils extends Logging {
  def createOrReplaceTempView(dataframe: DataFrame, tableType: TableType, name: String) = if (tableType == TableType.TEMP) dataframe.createOrReplaceTempView(name)

  def getLocationOfTable(spark: SparkSession, tableName: String): Option[String] = Try(spark.sql(s"describe formatted $tableName")
    .toDF //convert to dataframe will have 3 columns col_name,data_type,comment
    .filter(col("col_name") === "Location") //filter on column name
    .collect()(0)(1)
    .toString).toOption

  def repartitionDataframe(spark: SparkSession, dataframe: DataFrame, repartitionConfig: SparkRepartitionConfig, dataframeName: String = ""): (DataFrame, Option[PartitionOptimizerResult]) = {
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
}
