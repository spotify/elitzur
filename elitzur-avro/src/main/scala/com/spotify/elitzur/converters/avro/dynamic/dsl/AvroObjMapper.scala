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

package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroMapperException._
import org.apache.avro.Schema

import java.{util => ju}
import scala.annotation.tailrec
import scala.collection.mutable

object AvroObjMapper {
  private val mapToAvroFun: mutable.Map[String, Any => Any] = mutable.Map.empty[String, Any => Any]

  def getAvroFun(avroFieldPath: String, schema: Schema): Any => Any = {
    if (!mapToAvroFun.contains(avroFieldPath)) {
      val avroOperators = getAvroFilters(avroFieldPath, schema).map(_.ops)
      mapToAvroFun += (avroFieldPath -> combineFns(avroOperators))
    }
    mapToAvroFun(avroFieldPath)
  }

  @tailrec
  private[dsl] def getAvroFilters(
    path: String,
    avroSchema: Schema,
    accAvroOperators: List[AvroOperatorsHolder] = List.empty[AvroOperatorsHolder]
  ): List[AvroOperatorsHolder] = {
    val thisAvroOp = AvroFilterUtil.mapToFilters(path, avroSchema)
    val appendedAvroOp = accAvroOperators :+ thisAvroOp
    thisAvroOp.rest match {
      case Some(remainingPath) => getAvroFilters(remainingPath, thisAvroOp.schema, appendedAvroOp)
      case _ => appendedAvroOp
    }
  }

  private[dsl] def combineFns(fns: List[BaseFilter]): Any => Any =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopFilter().fn)
}

object AvroFilterUtil {
  private val PRIMITIVES: ju.EnumSet[Schema.Type] =
    ju.EnumSet.complementOf(ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION))

  // scalastyle:off cyclomatic.complexity
  def mapToFilters(path: String, schema: Schema): AvroOperatorsHolder = {
    val (field, op, rest) = pathToTokens(path)
    val fieldSchema = schema.getField(field)

    def mapToFilters(fieldSchema: Schema, fieldPos: Int, op: Option[String], rest: Option[String]):
    AvroOperatorsHolder = {
      fieldSchema.getType match {
        case _schema if PRIMITIVES.contains(_schema) =>
          new IndexFilterLogic(fieldSchema, fieldPos, rest).avroOp
        case Schema.Type.ARRAY =>
          new ArrayFilterLogic(fieldSchema.getElementType, fieldPos, rest, op).avroOp
        case Schema.Type.UNION =>
          fieldSchema.getTypes.removeIf(_.getType == Schema.Type.NULL)
          val innerSchema = fieldSchema.getTypes.get(0)
          val initFilter = mapToFilters(innerSchema, fieldPos, op, rest)
          new NullableFilterLogic(innerSchema, fieldPos, initFilter).avroOp
        case Schema.Type.MAP => throw new InvalidDynamicFieldException(UNSUPPORTED_MAP_SCHEMA)
      }
    }

    mapToFilters(fieldSchema.schema, fieldSchema.pos, op, rest)
  }
  // scalastyle:on cyclomatic.complexity

  private def pathToTokens(path: String): (String, Option[String], Option[String]) = {
    def strToOpt(str: String) :Option[String] = if (str.nonEmpty) Some(str) else None
    val token = '.'
    if (path.headOption.contains(token)) {
      val (fieldOps, rest) = path.drop(1).span(_ != token)
      val (field, op) = fieldOps.span(_.isLetterOrDigit)
      (field, strToOpt(op), strToOpt(rest))
    } else {
      throw new InvalidDynamicFieldException(MISSING_TOKEN)
    }
  }
}

case class AvroOperatorsHolder(ops: BaseFilter, schema: Schema, rest: Option[String])
