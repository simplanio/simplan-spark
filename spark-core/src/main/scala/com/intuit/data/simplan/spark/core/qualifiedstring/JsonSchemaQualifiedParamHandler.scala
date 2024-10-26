package com.intuit.data.simplan.spark.core.qualifiedstring

import com.intuit.data.simplan.global.domain.QualifiedParam
import com.intuit.data.simplan.global.qualifiedstring.QualifiedParamHandler
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.utils.JsonSchemaConvertor
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.File
import java.net.URI

/** @author Abraham, Thomas - tabraham1
  *         Created on 26-Apr-2022 at 2:34 PM
  */
class JsonSchemaQualifiedParamHandler extends QualifiedParamHandler[StructType] with Logging {
  override val qualifier: String = "schemaJson"

  override def resolve(qualifiedString: QualifiedParam): StructType = {
    logger.info(s"Resolving JsonSchemaQualifiedParamHandler for ${qualifiedString.string}")
    val jsonSchemaDefinition = qualifiedString.string match {
      case r if r.startsWith("classpath:") => ConfigFactory.parseResources(new URI(qualifiedString.string).getSchemeSpecificPart).root().render(ConfigRenderOptions.concise())
      case r if r.startsWith("file:")      => ConfigFactory.parseFile(new File(new URI(qualifiedString.string).getSchemeSpecificPart)).root().render(ConfigRenderOptions.concise())
      case r                               => r
    }
    DataType.fromJson(jsonSchemaDefinition).asInstanceOf[StructType]
  }
}
