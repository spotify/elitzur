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

import com.spotify.elitzur.converters.avro.qaas.utils.AvroObjMapperException.InvalidAvroException
import org.apache.avro.Schema

import scala.util.matching.Regex
import com.spotify.elitzur.converters.avro.qaas.utils._

object AvroObjMapper {
  private val hasNextNode: Regex = """^([a-zA-Z0-9]*)([^a-zA-Z\d\s:]+)(\w.*)$""".r
  private val isNode: Regex = """^([a-zA-Z0-9]*)([^a-zA-Z\d\s:]*)$""".r

  def extract(
    path: String, avroSchema: Schema,
    baseFunList: List[AvroRecursiveDataHolder] = List.empty[AvroRecursiveDataHolder]
    ): List[AvroRecursiveDataHolder] = {
    path match {
      case hasNextNode(field, combine, rest) =>
        val funThingy = new AvroWrappers(path, field, Some(rest))
        val combiner = OperatorUtils.matchMethod(combine)
        val avroOp = funThingy.schemaFun(avroSchema, combiner)
        if (avroOp.rest.isDefined) {
          extract(avroOp.rest.get, avroOp.schema, baseFunList :+ avroOp)
        } else {
          baseFunList :+ avroOp
        }
      case isNode(field, combine) =>
        val funThingy = new AvroWrappers(path, field)
        val combiner = OperatorUtils.matchEndMethod(combine)
        val avroOp = funThingy.schemaFun(avroSchema, combiner)
        baseFunList :+ avroOp
    }
  }
}

class AvroWrappers(path: String, field: String, rest: Option[String] = None) {

  private val PRIMITIVES: Set[Schema.Type] =
    Set(Schema.Type.STRING, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.BOOLEAN,
      Schema.Type.BYTES, Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.NULL)

  // scalastyle:off cyclomatic.complexity

  def schemaFun(schema: Schema, combiner: AvroWrapperOperator): AvroRecursiveDataHolder = {
    schema.getType match {
      case Schema.Type.RECORD =>
        val childSchema = schema.getField(field)
        AvroRecursiveDataHolder(
          GenericAvroObjWrapper(childSchema.pos, combiner), childSchema.schema, rest)
      case Schema.Type.UNION =>
        // assumes Union type is used specifically for nullability - remove the null schema
        schema.getTypes.removeIf(_.getType == Schema.Type.NULL)
        val innerAvroOp = schemaFun(schema.getTypes.get(0), combiner)
        AvroRecursiveDataHolder(
          UnionNullAvroObjWrapper(innerAvroOp.ops, combiner), innerAvroOp.schema, rest)
      case Schema.Type.ARRAY =>
        val arraySchema = schema.getElementType
        val innerOps = AvroObjMapper.extract(path, arraySchema)
        getAvroArrayOperation(arraySchema, field, innerOps, combiner)
      case schema if PRIMITIVES.contains(schema) => throw new InvalidAvroException(
        "Avro field cannot be retrieved from a primitive schema type")
      case Schema.Type.MAP | Schema.Type.FIXED | Schema.Type.ENUM => throw new InvalidAvroException(
        "Map, Enum and Fixed Avro schema types are currently not supported")}
  }
  // scalastyle:on cyclomatic.complexity

  // unfortunate function to get around Scala's weird array casting
  def getAvroArrayOperation(
    arraySchema: Schema, innerField: String, innerOps: List[AvroRecursiveDataHolder],
    c: AvroWrapperOperator
  ): AvroRecursiveDataHolder = {
    val innerFn = innerOps.map(_.ops).reduceLeftOption((f, g) => f + g).get.fn
    val remainingField = innerOps.lastOption.flatMap(_.rest)
    arraySchema.getField(innerField).schema.getType match {
      case Schema.Type.RECORD | Schema.Type.ARRAY => AvroRecursiveDataHolder(
        ArrayAvroObjWrapper(innerFn, CastArrayListOperation(), c), arraySchema, remainingField)
      case schema if PRIMITIVES.contains(schema) => AvroRecursiveDataHolder(
        ArrayAvroObjWrapper(innerFn, CastSeqWrapperOperation(), c), arraySchema, remainingField)
    }
  }
}

case class AvroRecursiveDataHolder(ops: AvroObjWrapper, schema: Schema, rest: Option[String])