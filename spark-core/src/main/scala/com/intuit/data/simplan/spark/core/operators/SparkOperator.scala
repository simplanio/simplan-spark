package com.intuit.data.simplan.spark.core.operators

import com.intuit.data.simplan.core.domain.operator._
import com.intuit.data.simplan.core.util.OpsMetricsUtils
import com.intuit.data.simplan.global.utils.ExecutionUtils
import com.intuit.data.simplan.global.utils.SimplanImplicits.{FromJsonImplicits, Pipe}
import com.intuit.data.simplan.logging.MetricConstants
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.{SparkCacheConfig, SparkOperatorOptionConfig}
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse, SparkOperatorSettings}
import com.intuit.data.simplan.spark.core.events.CountDistinctByColumns
import com.intuit.data.simplan.spark.core.utils.{DataframeUtils, StorageLevelUtils}

import java.{lang, util}
import scala.reflect.ClassTag

/** @author - Abraham, Thomas - tabaraham1
  *         Created on 8/19/21 at 1:42 PM
  */
abstract class SparkOperator[T <: OperatorConfig](
    appSupport: SparkAppContext,
    operatorContext: OperatorContext,
    settings: SparkOperatorSettings = SparkOperatorSettings()
)(implicit ct: ClassTag[T])
    extends BaseOperator[T](appSupport, operatorContext) {

  val operatorOptions: SparkOperatorOptionConfig =
    if (operatorContext.operatorDefinition.options != null)
      operatorContext.operatorDefinition.options.fromJson[SparkOperatorOptionConfig]
    else SparkOperatorOptionConfig()

  override def process(request: OperatorRequest): OperatorResponse = {
    appSupport.spark.sparkContext.setJobGroup(getJobGroup, getJobDescription)
    //If retries are not allowed for this operator processInternal and return
    if (!settings.allowRetries) return processInternal(request)
    operatorOptions.retry match {
      case Some(retryConfig) if retryConfig.isRetryEnabled =>
        ExecutionUtils.retry(retryConfig.maxAttempts, retryConfig.delayInSeconds, s"Retrying Operation ${operatorContext.taskName}(${operatorContext.operatorType})") {
          processInternal(request)
        }
      case _ => processInternal(request)
    }
  }

  private def processInternal(request: OperatorRequest): SparkOperatorResponse = {
    val response: SparkOperatorResponse = process(SparkOperatorRequest(request))
      .pipe(response => if (settings.allowRepartition) doRepartitioningIfDefined(response) else response)
      .pipe(response => if (settings.allowCaching) doCachingIfDefined(response) else response)
      .pipe(response => if (settings.allowMetrics) computeMetricsIfDefined(response) else response)
    appSupport.spark.sparkContext.clearJobGroup()
    response
  }

  def getJobDescription: String = operatorContext.operatorType.toString + "-" + operatorContext.operatorDefinition.operator
  def getJobGroup: String = operatorContext.taskName

  def doCachingIfDefined(response: SparkOperatorResponse): SparkOperatorResponse = {
    operatorOptions.cache.foreach {
      case SparkCacheConfig(persist, storageLevel, bocking) =>
        if (persist) {
          if (storageLevel != null) response.dataframes.foreach(_._2.persist(StorageLevelUtils.storageLevelMapper(storageLevel)))
          else response.dataframes.foreach(_._2.cache())
        } else response.dataframes.foreach(_._2.unpersist(Option(bocking).getOrElse(false)))
    }
    response
  }

  private def doRepartitioningIfDefined(response: SparkOperatorResponse): SparkOperatorResponse = {
    if (operatorOptions.repartition.isEmpty) return response

    val repartitionConfig = operatorOptions.repartition.get
    val repartitionedDataframes = response.dataframes.map {
      case (key, eachDataframe) => key -> DataframeUtils.repartition(appSupport.spark, eachDataframe, repartitionConfig, key)._1
    }
    response.copy(dataframes = repartitionedDataframes)
  }

  def process(request: SparkOperatorRequest): SparkOperatorResponse

  def computeMetricsIfDefined(response: SparkOperatorResponse): SparkOperatorResponse = {
    if (operatorOptions.metrics.isEmpty || !operatorOptions.metrics.get.isMetricsEnabled) return response
    val dataframe = response.dataframes.head._2
  //  if (operatorOptions.cache.isEmpty || !operatorOptions.cache.get.enabled) dataframe.cache()
    val metricsConfig = operatorOptions.metrics.get
    val aggs: util.Map[String, Object] = new util.HashMap[String, Object]()
    if (metricsConfig.isCountEnabled) aggs.put("count", new java.lang.Long(dataframe.count()))
    if (metricsConfig.isCountDistinctEnabled) aggs.put("countDistinct", new java.lang.Long(dataframe.distinct().count()))
    if (metricsConfig.getCountDistinctByColumns.nonEmpty) {
      val count = new lang.Long(dataframe.dropDuplicates(metricsConfig.getCountDistinctByColumns).count())
      val distinct = new CountDistinctByColumns().setCount(count)
      metricsConfig.getCountDistinctByColumns.foreach(each => distinct.addColumn(each))
      aggs.put("countDistinctWithColumns", distinct)
    }
    val opsEvent = OpsMetricsUtils.fromOperatorContext(
      operatorContext,
      s"Operator ${operatorContext.taskName}(${operatorContext.operatorType}) metrics",
      MetricConstants.Action.OPERATOR_METRICS,
      MetricConstants.Type.METRIC
    )
      .setEventData(aggs)
    appSupport.opsMetricsEmitter.info(opsEvent)
   // if (operatorOptions.cache.isEmpty || !operatorOptions.cache.get.enabled) dataframe.unpersist()
    response
  }
}
