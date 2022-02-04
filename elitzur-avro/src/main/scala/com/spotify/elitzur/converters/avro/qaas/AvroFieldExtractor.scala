/*
 * Copyright 2021 Spotify AB.
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

package com.spotify.elitzur.converters.avro.qaas

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import java.util
import scala.annotation.tailrec
import scala.language.implicitConversions
import scala.util.matching.Regex

object AvroFieldExtractor {
  private val hasLeafPath: Regex = """^([^.]*)\.(.*)""".r
  private val isLeaf: Regex = """^([^.]*)$""".r

  private val PRIMITIVES: Set[Schema.Type] =
    Set(Schema.Type.STRING, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.BOOLEAN,
      Schema.Type.BYTES, Schema.Type.FLOAT, Schema.Type.INT)

  private def evalArrayAccessor(
    field: String, rest: String, avroObject: Object
  ): Object= {
    val resList = new java.util.ArrayList[Object]
    val innerObjList = avroObject.asInstanceOf[java.util.ArrayList[GenericRecord]]
     innerObjList.forEach(
       obj => {
         val elemAvroObj = obj.get(field)
         val elemAvroSchema = obj.getSchema.getField(field).schema()
         val childElemAvroObj = recursiveFieldAccessor(rest, elemAvroObj, elemAvroSchema)
         childElemAvroObj match {
           case l: java.util.List[Object] => l.forEach(x => resList.add(x))
           case _ => resList.add(childElemAvroObj)
         }
       }
     )
    resList
  }

  // scalastyle:off cyclomatic.complexity
  @tailrec
  private def recursiveFieldAccessor(
    path: String, avroObject: Object, avroSchema: Schema
  ): Object = {
    path match {
      case hasLeafPath(field, rest) =>
        avroSchema.getType match {
          case Schema.Type.RECORD =>
            val innerObject = avroObject.asInstanceOf[GenericRecord].get(field)
            val innerSchema = avroSchema.getField(field).schema()
            recursiveFieldAccessor(rest, innerObject, innerSchema)
          case Schema.Type.ARRAY =>
            evalArrayAccessor(field, rest, avroObject)
          case schema if PRIMITIVES.contains(schema) =>
            throw new Exception("should not happen")
          case _ =>
            // Still need to implement Union/Map/ENUM Not sure about FIXED
            throw new Exception("oops not handled")
        }
      case isLeaf(field) =>
        avroSchema.getType match {
          case Schema.Type.RECORD => avroObject.asInstanceOf[GenericRecord].get(field)
          // This assumes that what can be within an array is only generics. need to handle the
          // case where the inside is non-primitive or generics (e.g. maps and arrays)
          case Schema.Type.ARRAY =>
            val resList = new util.ArrayList[Object]
            val avroObjList = avroObject.asInstanceOf[java.util.ArrayList[_]]
            avroObjList.forEach(x => resList.add(x.asInstanceOf[GenericRecord].get(field)))
            resList
          case schema if PRIMITIVES.contains(schema) =>
            throw new Exception("should not happen")
          case _ =>
            // Still need to implement Union/Map/ENUM Not sure about FIXED
            throw new Exception("oops not handled")
        }
    }
  }
  // scalastyle:off cyclomatic.complexity

  def getAvroValue(fieldValidationInput: String, avroRecord: GenericRecord): Object = {
    recursiveFieldAccessor(fieldValidationInput, avroRecord, avroRecord.getSchema)
  }

}
