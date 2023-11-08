package com.intuit.data.simplan.spark.core.operators

import com.intuit.data.simplan.core.domain.operator._
import com.intuit.data.simplan.global.utils.SimplanImplicits.{FromJsonImplicits, Pipe}
import com.intuit.data.simplan.spark.core.context.SparkAppContext
import com.intuit.data.simplan.spark.core.domain.operator.config.{SparkCacheConfig, SparkOperatorOptionConfig}
import com.intuit.data.simplan.spark.core.domain.{SparkOperatorRequest, SparkOperatorResponse, SparkOperatorSettings}
import com.intuit.data.simplan.spark.core.utils.{DataframeUtils, StorageLevelUtils}

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
    val response: SparkOperatorResponse = process(SparkOperatorRequest(request))
    val resolvedResponse = doCachingIfDefined(response)
      .pipe(doRepartitioningIfRequired)
    appSupport.spark.sparkContext.clearJobGroup()
    resolvedResponse
  }

  def getJobDescription: String = operatorContext.operatorType.toString + "-" + operatorContext.operatorDefinition.operator
  def getJobGroup: String = operatorContext.taskName

  private def doCachingIfDefined(response: SparkOperatorResponse): SparkOperatorResponse = {
    if (!settings.allowCaching) return response

    operatorOptions.cache.foreach {
      case SparkCacheConfig(persist, storageLevel, bocking) => {
        if (persist) {
          if (storageLevel != null) response.dataframes.foreach(_._2.persist(StorageLevelUtils.storageLevelMapper(storageLevel)))
          else response.dataframes.foreach(_._2.cache())
        } else {
          response.dataframes.foreach(_._2.unpersist(Option(bocking).getOrElse(false)))
        }
      }
    }
    response
  }

  private def doRepartitioningIfRequired(response: SparkOperatorResponse): SparkOperatorResponse = {
    if (!settings.allowRepartition) return response
    if (operatorOptions.repartition.isEmpty) return response

    val repartitionConfig = operatorOptions.repartition.get
    val repartitionedDataframes = response.dataframes.map {
      case (key, eachDataframe) => key -> DataframeUtils.repartitionDataframe(appSupport.spark, eachDataframe, repartitionConfig, key)._1
    }
    response.copy(dataframes = repartitionedDataframes)
  }

  def process(request: SparkOperatorRequest): SparkOperatorResponse
}
