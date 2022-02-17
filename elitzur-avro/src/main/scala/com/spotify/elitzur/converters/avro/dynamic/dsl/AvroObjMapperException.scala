package com.spotify.elitzur.converters.avro.dynamic.dsl

// TODO: please rename this class
object AvroObjMapperException {
  class InvalidAvroFieldSpecifiedException(msg: String) extends Exception(msg)

  class InvalidAvroException(msg: String) extends Exception(msg)
}
