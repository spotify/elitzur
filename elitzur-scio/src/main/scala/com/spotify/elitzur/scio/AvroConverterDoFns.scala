package com.spotify.elitzur.scio

import com.spotify.elitzur.converters.avro.{AvroConverter, AvroElitzurConversionUtils}
import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord, GenericRecordBuilder}
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

class FromAvroConverterDoFn[GR <: GenericRecord, T : Coder](ac: AvroConverter[T])
  extends DoFn[GR, T] with Serializable {
  @ProcessElement
  def processElement(c: DoFn[GR, T]#ProcessContext): Unit = {
    val e = c.element()
    c.output(ac.fromAvro(e, e.getSchema))
  }
}

class ToAvroConverterDoFn[T : Coder, GR <: GenericRecord : Coder : ClassTag](ac: AvroConverter[T])
  extends DoFn[T, GR] with Serializable {

  //We use reflection to get the schema here since it's only invoked per-worker instead of element
  val schemaSer: String = implicitly[ClassTag[GR]].runtimeClass.getMethod("getClassSchema")
    .invoke(null).asInstanceOf[Schema].toString(false)
  @transient lazy val schemaSerDe: Schema = new Schema.Parser().parse(schemaSer)

  @ProcessElement
  def processElement(c: DoFn[T, GR]#ProcessContext): Unit = {
    val e = c.element()
    c.output(ac.toAvro(e, schemaSerDe).asInstanceOf[GR])
  }
}

class ToAvroDefaultConverterDoFn[T : Coder, GR <: GenericRecord : Coder]
(defaultValueRecord: GR, ac: AvroConverter[T])
  extends DoFn[T, GR] with Serializable {

  @transient lazy val defaultGenericData: GenericData.Record =
    AvroElitzurConversionUtils.recordToGenericData(defaultValueRecord)
  @ProcessElement
  def processElement(c: DoFn[T, GR]#ProcessContext): Unit = {
    val e = c.element()
    c.output(ac.toAvroDefault(e, defaultGenericData).asInstanceOf[GR])
  }
}
