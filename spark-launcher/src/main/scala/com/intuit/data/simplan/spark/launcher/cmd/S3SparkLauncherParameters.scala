package com.intuit.data.simplan.spark.launcher.cmd

import org.apache.commons.lang.StringUtils

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Mar-2022 at 12:37 AM
  */
case class S3SparkLauncherParameters(
    basePath: String = StringUtils.EMPTY,
    jobPath: String = StringUtils.EMPTY,
    environment: Option[String] = None,
    arn: Option[String] = None,
    sessionPrefix: Option[String] = None,
    region: Option[String] = None
)

object S3SparkLauncherParameters {

  def parse(args: Array[String]): S3SparkLauncherParameters = {
    val argParser = new scopt.OptionParser[S3SparkLauncherParameters](s"spark-submit <configs> --class ${S3SparkLauncherParameters.getClass.getCanonicalName.replace("$", StringUtils.EMPTY)}") {
      head("Simplan ETL Spark Runner")
      override def errorOnUnknownArgument = false

      opt[String]('p', "basePath")
        .required()
        .action((x, c) => c.copy(basePath = x))
        .text(s"Pass Git organisation name as -o or --gitOrg")

      opt[String]('j', "jobPath")
        .required()
        .action((x, c) => c.copy(jobPath = x))
        .text(s"Pass Git repository name as -r or --gitRepo")

      opt[String]('e', "environment")
        .optional()
        .action((x, c) => c.copy(environment = Option(x)))
        .text(s"Pass job name as -j or --jobPath")

      opt[String]("arn")
        .optional()
        .action((x, c) => c.copy(arn = Option(x)))
        .text(s"Pass Git branch name as -b or --gitBranch")

      opt[String]("sessionPrefix")
        .optional()
        .action((x, c) => c.copy(sessionPrefix = Option(x)))
        .text(s"Pass Git branch name as -b or --gitBranch")

      opt[String]("region")
        .optional()
        .action((x, c) => c.copy(region = Option(x)))
        .text(s"Pass Git branch name as -b or --gitBranch")
    }
    argParser.parse(args.toSeq, S3SparkLauncherParameters()) match {
      case Some(config) => config
      case None         => throw new Exception("Invalid options")
    }
  }
}
