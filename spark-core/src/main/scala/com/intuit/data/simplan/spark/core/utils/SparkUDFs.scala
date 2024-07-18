package com.intuit.data.simplan.spark.core.utils

  import org.apache.spark.sql.Row
  import org.apache.spark.sql.expressions.UserDefinedFunction
  import org.apache.spark.sql.functions.udf

/** @author Abraham, Thomas - tabraham1
  *         Created on 30-Nov-2021 at 1:36 PM
  */
object SparkUDFs {

  val ArrayToMapUdf = udf[Map[String, Array[Byte]], Seq[Row]] { array =>
    array.collect {
      case element if element != null => (element.getAs[String]("key"), element.getAs[Array[Byte]]("value"))
    }.toMap
  }

  val ArrayToMapUdf2: UserDefinedFunction = udf[Map[String, Array[Byte]], Map[String, Array[Byte]]] { array =>
    array.collect {
      case element if element != null => (element._1, element._2)
    }
  }

}
