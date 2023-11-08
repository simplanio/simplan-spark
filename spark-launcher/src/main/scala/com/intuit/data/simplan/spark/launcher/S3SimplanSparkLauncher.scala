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

package com.intuit.data.simplan.spark.launcher

import com.intuit.data.simplan.core.aws.AmazonS3FileUtils
import com.intuit.data.simplan.core.context.DefaultRunParameters
import com.intuit.data.simplan.global.exceptions.SimplanExecutionException
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.service.SparkApplication
import com.intuit.data.simplan.spark.launcher.cmd.S3SparkLauncherParameters

import java.util.UUID
import scala.util.{Failure, Success}

/** @author Abraham, Thomas - tabraham1
  *         Created on 09-Aug-2023 at 1:40 PM
  */
object S3SimplanSparkLauncher extends Logging {

  def main(args: Array[String]): Unit = {
    val _commandParameters: S3SparkLauncherParameters = S3SparkLauncherParameters.parse(args)
    val copiedPath = copyFilesFromBasePath(_commandParameters) match {
      case Success(value)     => value
      case Failure(exception) => throw new SimplanExecutionException("Download of files from S3 failed", exception)
    }
    val jobName = _commandParameters.jobPath.split("/").last

    logger.info("Copied path: " + copiedPath)

    val resolvedPath = s"$copiedPath/${_commandParameters.jobPath}"
    logger.info("Resolved path: " + resolvedPath)

    val configs = Array(
      s"$resolvedPath/$jobName.conf",
      s"$resolvedPath/$jobName-${_commandParameters.environment.get}.conf"
    )
    logger.info("Configs: " + configs.mkString(","))
    val context = new SparkAppContext(configs)
    SparkApplication(context).run(new DefaultRunParameters())
  }

  def copyFilesFromBasePath(s3CmdArgs: S3SparkLauncherParameters) = {
    val amazonS3FileUtils =
      s3CmdArgs.arn match {
        case Some(_) =>
          AmazonS3FileUtils.create(
            s3CmdArgs.arn,
            s3CmdArgs.sessionPrefix.getOrElse("S3SimplanSparkLauncher" + UUID.randomUUID().toString),
            s3CmdArgs.region.getOrElse("US_WEST_2")
          )
        case None =>
          AmazonS3FileUtils.create()
      }
    val tempDir = System.getProperty("java.io.tmpdir")
    logger.info("Temp dir: " + tempDir)
    amazonS3FileUtils.downloadDirectoryToLocal(s3CmdArgs.basePath, tempDir)
  }

}
