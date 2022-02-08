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

import com.spotify.elitzur.converters.avro.qaas.AvroFieldExtractorExceptions.InvalidAvroFieldOperationException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import java.util
import scala.annotation.tailrec
import scala.collection.convert.Wrappers
import scala.language.implicitConversions
import scala.util.matching.Regex

// TODO: please rename this class
object AvroFieldExtractor {
  private val hasNextLeaf: Regex = """^([a-zA-Z0-9]*)\.(.*)""".r
  private val isLeafNode: Regex = """^([a-zA-Z0-9]*)$""".r
  private val hasNextLeafArray: Regex = """^([a-zA-Z0-9]*)\[\]\.(.*)""".r
  private val isLeafNodeArray: Regex = """^([a-zA-Z0-9]*)\[\]$""".r

  private val PRIMITIVES: Set[Schema.Type] =
    Set(Schema.Type.STRING, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.BOOLEAN,
      Schema.Type.BYTES, Schema.Type.FLOAT, Schema.Type.INT)

  private def evalArrayAccessor(
    field: String, rest: String, avroObject: Object, avroSchema: Schema
  ): Object = {
    val resList = new util.ArrayList[Object]
    avroSchema.getElementType.getType match {
      case Schema.Type.RECORD => avroObject match {
        case v: util.List[_] =>
          v.forEach(x => {
            val elemAvroObj = x.asInstanceOf[GenericRecord].get(field)
            val elemAvroSchema = elemAvroObj.asInstanceOf[GenericRecord].getSchema
            val childElemAvroObj = recursiveFieldAccessor(rest, elemAvroObj, elemAvroSchema)
            childElemAvroObj match {
              case l: util.List[Object] => l.forEach(x => resList.add(x))
              case _ => resList.add(childElemAvroObj)
            }
          })
        case _ => throw new Exception("not handled")
      }
      case _ => throw new Exception("not handled")
    }
    resList
  }

  // scalastyle:off cyclomatic.complexity
  private def evalArrayEndNodeAccessor(
    field: String, avroObject: Object, avroSchema: Schema, flatten: Boolean
  ): Object = {
    val resList = new util.ArrayList[Object]
    avroSchema.getElementType.getType match {
      case Schema.Type.RECORD => avroObject match {
        case v: util.List[_] =>
          v.forEach(x => {
            val elemAvroObj = x.asInstanceOf[GenericRecord].get(field)
            val elemAvroSchema = x.asInstanceOf[GenericRecord].getSchema.getField(field).schema
            // If the schema type is a primitive, then the element is not an array
            if (PRIMITIVES.contains(elemAvroSchema.getType)) {
              resList.add(elemAvroObj)
            } else {
              if (flatten) {
              // Assume each element is an array
              elemAvroSchema.getElementType.getType match {
                case Schema.Type.RECORD =>
                  elemAvroObj.asInstanceOf[util.ArrayList[Object]].forEach(y => resList.add(y))
                case schema if PRIMITIVES.contains(schema) =>
                  elemAvroObj.asInstanceOf[Wrappers.SeqWrapper[Object]].forEach(y => resList.add(y))
                case _ => throw new Exception("not handled")
              }
            } else {
                resList.add(elemAvroObj)
              }
            }
          })
          resList
        case _ => throw new Exception("not handled")
      }
      case _ => throw new Exception("not handled")
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def checkUnionSchema(
      path: String, avroObject: Object, avroSchema: Schema
  ): Object = {
    if (avroObject == null) {
      avroObject
    } else {
      // assumes Union type is used only for nullability, below code removes the null schema
      avroSchema.getTypes.removeIf(_.getType == Schema.Type.NULL)
      recursiveFieldAccessor(path, avroObject, avroSchema.getTypes.get(0))
    }
  }

  // scalastyle:off cyclomatic.complexity method.length
  @tailrec
  private def recursiveFieldAccessor(
    path: String, avroObject: Object, avroSchema: Schema
  ): Object = {
    path match {
      case hasNextLeaf(field, rest) =>
        avroSchema.getType match {
          case Schema.Type.RECORD =>
            val innerObject = avroObject.asInstanceOf[GenericRecord].get(field)
            val innerSchema = avroSchema.getField(field).schema()
            recursiveFieldAccessor(rest, innerObject, innerSchema)
          case Schema.Type.UNION =>
            checkUnionSchema(path,avroObject, avroSchema)
          case Schema.Type.ARRAY =>
            throw new InvalidAvroFieldOperationException(
              "[] is required for an array schema")
          case schema if PRIMITIVES.contains(schema) =>
            throw new InvalidAvroFieldOperationException(
              "Avro field cannot be retrieved from a primitive schema type")
          case _ =>
            throw new InvalidAvroFieldOperationException(
              "Map, Enum and Fixed Avro schema types are currently not supported")
        }
      case hasNextLeafArray(field, rest) =>
        val childObj = avroObject.asInstanceOf[GenericRecord].get(field)
        val childSchema = avroObject.asInstanceOf[GenericRecord].getSchema.getField(field).schema
        childSchema.getType match {
          case Schema.Type.ARRAY =>
            rest match {
              case hasNextLeaf(cField, cRest) =>
                evalArrayAccessor(cField, cRest, childObj, childSchema)
              case isLeafNode(cField) =>
                evalArrayEndNodeAccessor(cField, childObj, childSchema, flatten = false)
              case isLeafNodeArray(cField) =>
                evalArrayEndNodeAccessor(cField, childObj, childSchema, flatten = true)
            }
          case _ =>
            throw new InvalidAvroFieldOperationException(
              "usage of [] is valid for only array schema")
        }
      case isLeafNode(field) =>
        avroSchema.getType match {
          case Schema.Type.RECORD =>
            avroObject.asInstanceOf[GenericRecord].get(field)
          case Schema.Type.UNION =>
            checkUnionSchema(path,avroObject, avroSchema)
          case Schema.Type.ARRAY =>
            evalArrayEndNodeAccessor(field, avroObject, avroSchema, flatten = false)
          case schema if PRIMITIVES.contains(schema) =>
            throw new InvalidAvroFieldOperationException(
              "Avro field cannot be retrieved from a primitive schema type")
          case _ =>
            throw new InvalidAvroFieldOperationException(
              "Map, Enum and Fixed Avro schema types are currently not supported")
        }
      case isLeafNodeArray(field) =>
        avroSchema.getType match {
          case Schema.Type.ARRAY =>
            evalArrayEndNodeAccessor(field, avroObject, avroSchema, flatten = true)
          case _ =>
            throw new InvalidAvroFieldOperationException(
              "usage of [] is valid for only array schema")
        }
    }
  }
  // scalastyle:on method.length cyclomatic.complexity

  def getAvroValue(fieldValidationInput: String, avroRecord: GenericRecord): Object = {
    recursiveFieldAccessor(fieldValidationInput, avroRecord, avroRecord.getSchema)
  }

}
