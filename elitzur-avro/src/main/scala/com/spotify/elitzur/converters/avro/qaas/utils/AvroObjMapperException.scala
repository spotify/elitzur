package com.spotify.elitzur.converters.avro.qaas.utils

// TODO: please rename this class
object AvroObjMapperException {
  class InvalidAvroFieldSpecifiedException(msg: String, e: Throwable) extends Exception(msg, e)

  class InvalidAvroException(msg: String) extends Exception(msg)
}
