package com.spotify.elitzur.converters.avro.dynamic.dsl

object AvroMapperException {
  class InvalidDynamicFieldException(msg: String) extends Exception(msg)

  final val MISSING_TOKEN =
    "Leading '.' missing in the arg. Please prepend '.' to the arg"

  final val UNSUPPORTED_MAP_SCHEMA =
    "Map schema not supported. Please use Magnolia version of Elitzur."

}
