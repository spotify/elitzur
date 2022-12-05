package com.spotify.elitzur.converters.avro.dynamic.dsl.core

object SchemaToAccessorOpsExceptionMsg {
  class InvalidDynamicFieldException(msg: String) extends Exception(msg)

  final val MISSING_TOKEN =
    "Leading '.' missing in the arg. Please prepend '.' to the arg"

  final val UNSUPPORTED_MAP_SCHEMA =
    "Map schema not supported. Please use Magnolia version of Elitzur."

  final val INVALID_UNION_SCHEMA =
    "Union schemas containing more than one non-null schemas is not supported."

  final val INVALID_SCHEMA =
    "schemas containing more than one non-null schemas is not supported."

  final val MISSING_ARRAY_TOKEN =
    """
      |Missing `[]` token for an array fields. All array fields should have `[]` token provided
      |in the input.
      |""".stripMargin

  def unsupportedSchemaMessage(schema: String): String =
    s"The schema type $schema is not supported in Elitzur-Runner."
}
