/*
 * Copyright 2020 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
