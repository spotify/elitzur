package com.spotify.elitzur.converters.avro.dynamic.schema

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import java.{util => ju}

sealed trait SchemaType[T]

object SchemaType {
  implicit val avroSchema: SchemaType[Schema] = new SchemaType[Schema] {}
  implicit val bqSchema: SchemaType[BqSchema] = new SchemaType[BqSchema] {}
}

object SchemaTypeUtil {
  def getFieldFromSchema[T: SchemaType](t: T, fieldName: String): Any => Any = t match {
    case _: Schema => (o: Any) => o.asInstanceOf[GenericRecord].get(fieldName)
    case _: BqSchema => (o: Any) => o.asInstanceOf[ju.Map[String, Any]].get(fieldName)
  }
}
