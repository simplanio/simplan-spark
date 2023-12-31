package com.intuit.data.simplan.spark.core.utils

import com.intuit.data.simplan.common.files.{FileListing, FileUtils}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.io.{BufferedReader, InputStreamReader}
import scala.util.Try

/** @author Abraham, Thomas - tabraham1
  *         Created on 15-Nov-2021 at 10:35 AM
  */
class SparkFileUtils(spark: SparkSession) extends FileUtils {
  val hadoopConfig: Configuration = spark.sparkContext.hadoopConfiguration
  val filesystem: FileSystem = FileSystem.newInstance(hadoopConfig)

  override def readContent(path: String, charset: String): String = {
    val fileToReadBuffer: BufferedReader = new BufferedReader(new InputStreamReader(filesystem.open(new Path(path))))
    Stream.continually(fileToReadBuffer.readLine()).takeWhile(_ != null).mkString("\n")
  }

  override def exists(path: String): Boolean = filesystem.exists(new Path(path))

  override def writeContent(path: String, content: String): Boolean = ???

  override def copy(sourcePath: String, destinationPath: String): Boolean = ???

  override def list(path: String, recursive: Boolean, filter: FileListing => Boolean): List[FileListing] = ???

  override def getCountAndSize(path: String): (Long, Long) = ???

  override val schemes: List[String] = List.empty
}
