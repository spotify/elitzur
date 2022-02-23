package com.spotify.elitzur.converters.avro.dynamic.dsl

object AvroMapperException {
  class InvalidDynamicFieldException(msg: String) extends Exception(msg)

  final val MISSING_TOKEN =
    "Leading '.' missing in the arg. Please prepend '.' to the arg"

  final val UNSUPPORTED_MAP_SCHEMA =
    "Map schema not supported. Please use Magnolia version of Elitzur."

  final val MISSING_ARRAY_TOKEN =
    """
      |Missing `[]` for an array field. Appending `[]` is required for all array fields except
      |in the case where the last field in the arg is an array.
      |""".stripMargin

}
