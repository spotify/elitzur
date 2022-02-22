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
      val avroOperators = getAvroOperators(avroFieldPath, schema).map(_.ops)
      mapToAvroFun += (avroFieldPath -> combineFns(avroOperators))
    }
    mapToAvroFun(avroFieldPath)
  }

  @tailrec
  private[dsl] def getAvroOperators(
    path: String,
    avroSchema: Schema,
    accAvroOperators: List[AvroOperatorsHolder] = List.empty[AvroOperatorsHolder]
  ): List[AvroOperatorsHolder] = {
    val thisAvroOp = AvroOperatorUtil.mapToFilters(path, avroSchema)
    val appendedAvroOp = accAvroOperators :+ thisAvroOp
    thisAvroOp.rest match {
      case Some(remainingPath) => getAvroOperators(remainingPath, thisAvroOp.schema, appendedAvroOp)
      case _ => appendedAvroOp
    }
  }

  private[dsl] def combineFns(fns: List[BaseFilter]): Any => Any =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopFilter().fn)
}

object AvroOperatorUtil {
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
          new IndexFilterLogic(fieldSchema, fieldPos, rest).apply
        case Schema.Type.ARRAY =>
          new ArrayFilterLogic(fieldSchema.getElementType, fieldPos, rest, op).apply
        case Schema.Type.UNION =>
          fieldSchema.getTypes.removeIf(_.getType == Schema.Type.NULL)
          val innerSchema = fieldSchema.getTypes.get(0)
          val firstFilter = mapToFilters(innerSchema, fieldPos, op, rest)
          new NullableFilterLogic(innerSchema, fieldPos, firstFilter).apply
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

class IndexFilterLogic(schema: Schema, pos: Int, rest: Option[String]) {
  val filter: IndexFilter = IndexFilter(pos)
  def apply: AvroOperatorsHolder = AvroOperatorsHolder(filter, schema, rest)
}

class NullableFilterLogic(schema: Schema, pos: Int, firstFilter: AvroOperatorsHolder) {
   val (innerOps, remainingPath) = recursiveInnerOperation(schema, firstFilter)

  val filter: NullableFilter = NullableFilter(pos, innerOps)
  def apply: AvroOperatorsHolder = AvroOperatorsHolder(filter, schema, remainingPath)

  private def recursiveInnerOperation(
    innerSchema: Schema, firstFilter: AvroOperatorsHolder): (Any => Any, Option[String]) = {
    if (firstFilter.rest.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroOperators(firstFilter.rest.get, innerSchema)
      val innerOps = AvroObjMapper.combineFns((firstFilter +: recursiveResult).map(_.ops))
      val remainingPath = recursiveResult.lastOption.flatMap(_.rest)
      (innerOps, remainingPath)
    } else {
      (firstFilter.ops.fn, None)
    }
  }
}

class ArrayFilterLogic(
  arrayElemSchema: Schema, pos: Int, rest: Option[String], op: Option[String]) {
  val (arrayInnerOps, flatten, remainingField, isEndArray) =
    recursiveInnerOperation(arrayElemSchema, rest, op)

  val filter: ArrayFilter = ArrayFilter(pos, arrayInnerOps, flatten, isEndArray)
  def apply: AvroOperatorsHolder = AvroOperatorsHolder(filter, arrayElemSchema, remainingField)

  private def recursiveInnerOperation(
    innerSchema: Schema, remainingField: Option[String], opToken: Option[String]
  ): (Any => Any, Boolean, Option[String], Boolean) = {
    if (remainingField.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroOperators(remainingField.get, innerSchema)
      // innerOps represents the list of operators to be applied to each element in an array
      val innerOps = AvroObjMapper.combineFns(recursiveResult.map(_.ops))
      // flattenFlag is true if one of the internal operation types is an array-based operation
      val flattenFlag = recursiveResult.map(_.ops).exists(_.isInstanceOf[ArrayFilter])
      // remainingPath represents the remaining user provided string to be traversed
      val remainingPath = recursiveResult.lastOption.flatMap(_.rest)
      (innerOps, flattenFlag, remainingPath, false)
    } else {
      (NoopFilter().fn, opToken.contains("[]"), None, true)
    }
  }
}