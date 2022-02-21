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
    val (field, op, rest) = pathToTokens(path)
    val thisAvroOp = AvroOperatorUtil.mapToFilters(avroSchema, field, op, rest)
    val appendedAvroOp = accAvroOperators :+ thisAvroOp
    thisAvroOp.rest match {
      case Some(remainingPath) => getAvroOperators(remainingPath, thisAvroOp.schema, appendedAvroOp)
      case _ => appendedAvroOp
    }
  }

  private[dsl] def combineFns(fns: List[BaseFilter]): Any => Any =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopFilter().fn)

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

object AvroOperatorUtil{
  private val PRIMITIVES: ju.EnumSet[Schema.Type] =
    ju.EnumSet.complementOf(ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP))

  // scalastyle:off cyclomatic.complexity
  def mapToFilters(avroSchema: Schema, field: String, op: Option[String], rest: Option[String]):
  AvroOperatorsHolder = {
    val innerField = avroSchema.getField(field)
    val (normSchema, nullable) = sanitizeSchema(innerField.schema)

    normSchema.getType match {
      case _schema if PRIMITIVES.contains(_schema) =>
        val op = GenericRecordFilter(innerField.pos)
        AvroOperatorsHolder(nullableOpIfNeeded(op, nullable), normSchema, rest)
      case Schema.Type.ARRAY =>
        val arrayElemSchema = normSchema.getElementType
        val (arrayInnerOps, flatten, remainingField) =
          recursiveArrayInnerOperator(arrayElemSchema, rest)
        val filter = ArrayFilter(innerField.pos(), arrayInnerOps, flatten)
        AvroOperatorsHolder(filter, arrayElemSchema, remainingField)
      case Schema.Type.MAP => throw new InvalidDynamicFieldException(UNSUPPORTED_MAP_SCHEMA)
    }
  }
  // scalastyle:on cyclomatic.complexity

  private def sanitizeSchema(schema: Schema): (Schema, Boolean) = {
    if (schema.getType != Schema.Type.UNION) {
      (schema, false)
    } else {
      schema.getTypes.removeIf(_.getType == Schema.Type.NULL)
      (schema.getTypes.get(0), true)
    }
  }

  private def nullableOpIfNeeded(op: BaseFilter, nullable: Boolean): BaseFilter = {
    if (nullable) NullableFilter(op) else op
  }

  private def recursiveArrayInnerOperator(
    innerSchema: Schema, remainingField: Option[String]
  ): (Any => Any, Boolean, Option[String]) = {
    if (remainingField.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroOperators(remainingField.get, innerSchema)
      // innerOps represents the list of operators to be applied to each element in an array
      val innerOps = AvroObjMapper.combineFns(recursiveResult.map(_.ops))
      // flattenFlag is true if one of the internal operation types is an array-based operation
      val flattenFlag = recursiveResult.map(_.ops).exists(_.isInstanceOf[ArrayFilter])
      // remainingPath represents the remaining user provided string to be traversed
      val remainingPath = recursiveResult.lastOption.flatMap(_.rest)
      (innerOps, flattenFlag, remainingPath)
    } else {
      (NoopFilter().fn, false, None)
    }
  }

}

case class AvroOperatorsHolder(ops: BaseFilter, schema: Schema, rest: Option[String])
