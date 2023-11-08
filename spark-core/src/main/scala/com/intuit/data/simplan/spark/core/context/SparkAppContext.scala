package com.intuit.data.simplan.spark.core.context

import com.intuit.data.simplan.common.files.FileUtils
import com.intuit.data.simplan.core.aws.AmazonS3FileUtils
import com.intuit.data.simplan.core.context.{AppContext, InitContext}
import com.intuit.data.simplan.global.qualifiedstring.QualifiedParameterManager
import com.intuit.data.simplan.spark.core.config.SparkSystemConfiguration
import com.intuit.data.simplan.spark.core.domain.Constants.ENABLE_HIVE_SUPPORT
import com.intuit.data.simplan.spark.core.monitoring.StreamMonitoring
import com.intuit.data.simplan.spark.core.qualifiedstring.{DDLSchemaQualifiedParamHandler, JsonSchemaQualifiedParamHandler}
import com.intuit.data.simplan.spark.core.utils.SparkFileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, SparkFiles}
import org.slf4j.LoggerFactory

/** @author - Abraham, Thomas - tabaraham1
  *         Created on 8/19/21 at 1:21 AM
  */
class SparkAppContext(val initContext: InitContext) extends AppContext(initContext) {
  private lazy val logger = LoggerFactory.getLogger(classOf[SparkAppContext])
  def this(configs: Array[String]) = this(InitContext(userConfigs = configs))

  addDefaultAppContextConfigFile("spark-operator-mappings.conf")
  addDefaultAppContextConfigFile("spark-config-base.conf")
  QualifiedParameterManager.registerHandler(new DDLSchemaQualifiedParamHandler)
  QualifiedParameterManager.registerHandler(new JsonSchemaQualifiedParamHandler)

  private lazy val sparkSystemConfiguration: SparkSystemConfiguration = appContextConfig.getSystemConfigAs[SparkSystemConfiguration]("spark")

  lazy val spark: SparkSession = {
    sparkSystemConfiguration.properties.foreach(each => System.setProperty(each._1, each._2))
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .setAll(sparkSystemConfiguration.properties)
      .setAppName(appContextConfig.application.name)
    val sparkBuilder = SparkSession.builder().appName(appContextConfig.application.name).config(sparkConf)
    val attachHiveSupport =
      if (sparkSystemConfiguration.options != null
        && sparkSystemConfiguration.options.contains(ENABLE_HIVE_SUPPORT)
        && sparkSystemConfiguration.options(ENABLE_HIVE_SUPPORT).toBoolean) {
        sparkBuilder.enableHiveSupport()
      } else sparkBuilder
    val session = attachHiveSupport.getOrCreate()
    registerCustomUDFs(session)
    session
  }
//  StreamMonitoring(this).attachMonitoringListener()

  private def registerCustomUDFs(spark: SparkSession): Unit = {
    if (sparkSystemConfiguration.udfs != null)
      sparkSystemConfiguration.udfs.foreach {
        case (functionName, udfConfig) =>
          val functionDefinition = udfConfig.functionDefinition(functionName)
          logger.info(s"Registering Custom UDF($functionName) within Simplan : $functionDefinition")
          spark.sql(functionDefinition)
      }
  }

  lazy val sc: SparkContext = spark.sparkContext
  override lazy val fileUtils: FileUtils = new SparkFileUtils(spark)
}

object SparkAppContext {

  def apply(userConfigs: Array[String]): SparkAppContext = {
    new SparkAppContext(InitContext(userConfigs))
  }

}
