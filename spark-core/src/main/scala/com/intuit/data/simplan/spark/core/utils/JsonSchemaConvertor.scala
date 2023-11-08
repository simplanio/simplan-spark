package com.intuit.data.simplan.spark.core.utils

import com.fasterxml.jackson.databind.JsonNode
import com.intuit.data.simplan.common.exceptions.SimplanConfigException
import com.intuit.data.simplan.global.json.JacksonJsonMapper
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.types._

import java.io.File
import scala.collection.convert.ImplicitConversions.`iterator asScala`

/**
 * @author Kiran Hiremath
 */
class JsonSchemaConvertor extends Serializable {

  def convert(schema: String): StructType = {
    jsonSchemaToStructType(new JacksonJsonMapper().fromJsonAsTree(schema))
  }

  def jsonSchemaToStructType(schema: JsonNode): StructType = {
    val properties: JsonNode = schema.get("properties")
    val requiredFields = if (schema.has("required")) Some(schema.get("required")) else None
    val fields = properties.fields().toSeq.map(field => {
      val fieldName = field.getKey
      val fieldValue = field.getValue
      val fieldType = fieldValue.get("type").asText()
      val nullable = if (requiredFields.isDefined)
        !requiredFields.get.elements().map(x => x.asText()).toSeq.contains(fieldName) else true
      val metadata = if (fieldValue.has("description")) {
        new MetadataBuilder()
          .putString("description", fieldValue.get("description").asText())
          .build()
      } else Metadata.empty

      fieldType match {
        case "string" => StructField(fieldName, StringType, nullable, metadata)
        case "integer" => StructField(fieldName, IntegerType, nullable, metadata)
        case "number" => StructField(fieldName, DoubleType, nullable, metadata)
        case "boolean" => StructField(fieldName, BooleanType, nullable, metadata)
        case "array" => StructField(fieldName, ArrayType(jsonSchemaToStructType(fieldValue.get("items"))), nullable, metadata)
        case "object" => StructField(fieldName, jsonSchemaToStructType(fieldValue), nullable, metadata)
        case _ => throw new SimplanConfigException(s"Unknown fieldType in JsonSchema: $fieldType. Accepted values string,integer,number,boolean,array,object")
      }
    })
    StructType(fields)
  }
}
