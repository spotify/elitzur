package com.spotify.elitzur.converters.avro.dynamic.validator.core

import com.google.api.services.bigquery.model.TableRow
import org.apache.avro.generic.GenericRecord

import java.{util => ju}

sealed trait RecordType[T]

object RecordType {
  implicit val avroRecord: RecordType[GenericRecord] = new RecordType[GenericRecord] {}
  implicit val bqRecord: RecordType[TableRow] = new RecordType[TableRow] {}
}

