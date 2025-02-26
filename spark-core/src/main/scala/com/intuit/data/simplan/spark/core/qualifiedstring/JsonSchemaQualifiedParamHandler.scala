package com.intuit.data.simplan.spark.core.qualifiedstring

import com.intuit.data.simplan.global.domain.QualifiedParam
import com.intuit.data.simplan.global.qualifiedstring.QualifiedParamHandler
import com.intuit.data.simplan.logging.Logging
import com.intuit.data.simplan.spark.core.utils.JsonSchemaConvertor
import com.typesafe.config.{ConfigFactory, ConfigRenderOptions}
import org.apache.spark.sql.types.{DataType, StructType}

import java.io.File
import java.net.URI
import scala.util.Try

/** @author Abraham, Thomas - tabraham1
  *         Created on 26-Apr-2022 at 2:34 PM
  */
class JsonSchemaQualifiedParamHandler extends QualifiedParamHandler[StructType] with Logging {
  override val qualifier: String = "schemaJson"

  override def resolve(qualifiedString: QualifiedParam): StructType = {
    logger.info(s"Resolving JsonSchemaQualifiedParamHandler for ${qualifiedString.string}")
    val schemeSpecificPart = new URI(qualifiedString.string).getSchemeSpecificPart
    val jsonSchemaDefinition = qualifiedString.string match {
      case r if r.startsWith("classpath:") => ConfigFactory.parseResources(schemeSpecificPart).root().render(ConfigRenderOptions.concise())
      case r if r.startsWith("file:")      => ConfigFactory.parseFile(new File(schemeSpecificPart)).root().render(ConfigRenderOptions.concise())
      case r                               => r
    }
    Try(DataType.fromJson(jsonSchemaDefinition).asInstanceOf[StructType]) match {
      case scala.util.Success(value) => value
      case scala.util.Failure(exception) =>
        logger.error(s"Failed to parse json schema for ${qualifiedString.qualifiedString} - Json Schema Definition: $jsonSchemaDefinition", exception)
        throw exception
    }
  }
}
