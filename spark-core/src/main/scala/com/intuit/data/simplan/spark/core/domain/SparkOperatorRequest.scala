package com.intuit.data.simplan.spark.core.domain

import com.intuit.data.simplan.core.domain.operator.OperatorRequest
import com.intuit.data.simplan.spark.core.operators.SparkOperator
import io.delta.tables.DeltaTable
import org.apache.spark.sql.DataFrame

/** @author Abraham, Thomas - tabraham1
  *         Created on 18-Nov-2021 at 9:11 PM
  */
case class SparkOperatorRequest(operatorRequest: OperatorRequest) extends OperatorRequest(operatorRequest.xCom) {

  lazy val sparkOperatorResponses: Map[String, SparkOperatorResponse] = xCom
    .filter(_._2 operatorExtends classOf[SparkOperator[_]])
    .map(each => (each._1, each._2.operatorResponse.asInstanceOf[SparkOperatorResponse]))

  lazy val dataframes: Map[String, DataFrame] = sparkOperatorResponses.flatMap(_._2.dataframes) ++ sparkOperatorResponses.flatMap(_._2.deltaTable.map(each => (each._1, each._2.toDF)))

  lazy val deltaTable: Map[String, DeltaTable] = sparkOperatorResponses.flatMap(_._2.deltaTable)

}
