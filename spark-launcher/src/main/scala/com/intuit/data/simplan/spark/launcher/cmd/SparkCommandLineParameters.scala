package com.intuit.data.simplan.spark.launcher.cmd

import com.intuit.data.simplan.core.domain.ScriptCommandLineParameters
import org.apache.commons.lang.StringUtils

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Mar-2022 at 12:37 AM
  */
case class SparkCommandLineParameters(
    override val configs: Array[String] = Array.empty,
    override val script: String = StringUtils.EMPTY,
    override val appName: String = "SimplanSparkApp",
    override val parentAppName: Option[String] = None,
    override val project: Option[String] = None,
    override val assetId: Option[String] = None,
    override val opsOwner: Option[String] = None,
    override val businessOwner: Option[String] = None,
    override val runId: Option[String] = None,
    override val environment: Option[String] = None,
    override val orchestratorId: Option[String] = None,
    override val orchestrator: Option[String] = None
) extends ScriptCommandLineParameters

object SparkCommandLineParameters {

  def parse(args: Array[String]): SparkCommandLineParameters = {
    val argParser = new scopt.OptionParser[SparkCommandLineParameters](s"spark-submit <configs> --class ${SparkCommandLineParameters.getClass.getCanonicalName.replace("$", StringUtils.EMPTY)}") {
      head("Simplan ETL Spark Runner")
      override def errorOnUnknownArgument = false

      opt[String]('c', "config")
        .optional()
        .action((x, c) => c.copy(configs = x.split(",").map(_.trim)))
        .text(s"Pass Simplan Config file as -c or --config")

      opt[String]('s', "script")
        .required()
        .action((x, c) => c.copy(script = x))
        .text(s"Please provide Sql Script using --script or -s")

      opt[String]('n', "appName")
        .required()
        .action((x, c) => c.copy(appName = x))
        .text(s"Please provide App Name using --appName or -n")

      opt[String]("project")
        .required()
        .action((x, c) => c.copy(project = Option(x)))

      opt[String]('p', "parentAppName")
        .required()
        .action((x, c) => c.copy(parentAppName = Option(x)))
        .text(s"Please provide pipeline name using --pipeline or -p")

      opt[String]("assetId")
        .required()
        .action((x, c) => c.copy(assetId = Option(x)))

      opt[String]("opsOwner")
        .optional()
        .action((x, c) => c.copy(opsOwner = Option(x)))

      opt[String]("externalOrchestrationId")
        .optional()
        .action((x, c) => c.copy(orchestratorId = Option(x)))

      opt[String]("orchestratorId")
        .optional()
        .action((x, c) => c.copy(orchestratorId = Option(x)))

      opt[String]("orchestrator")
        .optional()
        .action((x, c) => c.copy(orchestrator = Option(x)))

      opt[String]("environment")
        .optional()
        .action((x, c) => c.copy(environment = Option(x)))

      opt[String]("businessOwner")
        .optional()
        .action((x, c) => c.copy(businessOwner = Option(x)))

    }
    argParser.parse(args.toSeq, SparkCommandLineParameters()) match {
      case Some(config) => config
      case None         => throw new Exception("Invalid options")
    }
  }
}
