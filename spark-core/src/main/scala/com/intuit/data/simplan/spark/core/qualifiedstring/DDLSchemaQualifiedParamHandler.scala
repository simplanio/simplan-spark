package com.intuit.data.simplan.spark.core.qualifiedstring

import com.intuit.data.simplan.global.domain.QualifiedParam
import com.intuit.data.simplan.global.qualifiedstring.QualifiedParamHandler
import org.apache.spark.sql.types.{DataType, StructType}

/** @author Abraham, Thomas - tabraham1
  *         Created on 26-Apr-2022 at 2:34 PM
  */
class DDLSchemaQualifiedParamHandler extends QualifiedParamHandler[StructType] {
  override val qualifier: String = "schemaDDL"

  override def resolve(qualifiedString: QualifiedParam): StructType = DataType.fromDDL(qualifiedString.string).asInstanceOf[StructType]
}
