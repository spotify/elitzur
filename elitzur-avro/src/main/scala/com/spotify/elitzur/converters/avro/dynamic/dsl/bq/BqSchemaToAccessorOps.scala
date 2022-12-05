package com.spotify.elitzur.converters.avro.dynamic.dsl.bq

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.SchemaToAccessorOps
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.SchemaToAccessorOpsExceptionMsg._
import com.spotify.elitzur.converters.avro.dynamic.schema.{BqField, BqSchema}

object BqSchemaToAccessorOps extends SchemaToAccessorOps[BqSchema] {
  override def isRequired(fieldSchema: BqSchema): Boolean = {
    fieldSchema.fieldMode == BqField.Required
  }

  override def isRepeated(fieldSchema: BqSchema): Boolean = {
    fieldSchema.fieldMode == BqField.Repeated
  }

  override def isNullable(fieldSchema: BqSchema): Boolean = {
    fieldSchema.fieldMode == BqField.Nullable
  }

  override def isNotSupported(fieldSchema: BqSchema): Boolean = {
    throw new InvalidDynamicFieldException(INVALID_SCHEMA)
  }

  override def getFieldSchema(schema: BqSchema, fieldName: String): BqSchema = {
    val fieldSchema = schema.fieldSchemas.stream()
      .filter(_.getName == fieldName)
      .findFirst()
      .orElseThrow(() => throw new InvalidDynamicFieldException(
        s"$fieldName not found in ${schema.fieldSchemas.toString}"))
    BqSchema(fieldSchema.getFields, fieldSchema.getMode)
  }

  override def getNonNullableFieldSchema(schema: BqSchema): BqSchema =
    schema.copy(fieldMode = BqField.Required)

  override def getElemFieldSchema(schema: BqSchema): BqSchema =
    schema.copy(fieldMode = BqField.Required)
}
