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

import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroObjMapperException._
import org.apache.avro.Schema

import java.{util => ju}
import scala.util.matching.Regex

object AvroObjMapper {

  @SuppressWarnings(Array("org.wartremover.warts.Var"))
  var mapToAvroFun: Map[String, Any => Any] = Map.empty

  def getAvroFun(avroFieldPath: String, schema: Schema): Any => Any = {
    if (mapToAvroFun.contains(avroFieldPath)) {
      mapToAvroFun(avroFieldPath)
    } else {
      val avroOperators = getAvroOperators(avroFieldPath, schema)
      mapToAvroFun += (avroFieldPath -> combineFns(avroOperators))
      mapToAvroFun(avroFieldPath)
    }
  }

  def getAvroOperators(
    path: String,
    avroSchema: Schema,
    accAvroOperators: List[AvroOperatorsHolder] = List.empty[AvroOperatorsHolder]
  ): List[AvroOperatorsHolder] = {
    val hasNextNode: Regex = """^([a-zA-Z0-9]*)([^a-zA-Z\d\s:]+)(\w.*)$""".r
    val isNode: Regex = """^([a-zA-Z0-9]*)([^a-zA-Z\d\s:]*)$""".r

    path match {
      case hasNextNode(field, op, rest) =>
        val thisAvroOp = AvroOperatorUtil.mapToAvroOp(avroSchema, field, op, Some(rest))
        if (thisAvroOp.rest.isDefined) {
          getAvroOperators(thisAvroOp.rest.get, thisAvroOp.schema, accAvroOperators :+ thisAvroOp)
        } else {
          accAvroOperators :+ thisAvroOp
        }
      case isNode(field, op) =>
        val thisAvroOp = AvroOperatorUtil.mapToAvroOp(avroSchema, field, op)
        accAvroOperators :+ thisAvroOp
    }
  }

  def combineFns(fns: List[AvroOperatorsHolder]): Any => Any =
    fns.map(_.ops.fn).reduceLeftOption((f, g) => f andThen g).get
}

object AvroOperatorUtil{
  private val PRIMITIVES: ju.EnumSet[Schema.Type] =
    ju.EnumSet.complementOf(ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP))

  // scalastyle:off cyclomatic.complexity
  def mapToAvroOp(avroSchema: Schema, field: String, op: String, rest: Option[String] = None):
  AvroOperatorsHolder = {
    val innerField = avroSchema.getField(field)
    val (normSchema, nullable) = sanitizeSchema(innerField.schema)
    validateSchemaAndInput(op, normSchema)

    normSchema.getType match {
      case _schema if PRIMITIVES.contains(_schema) =>
        val op = GenericRecordOperator(innerField.pos)
        AvroOperatorsHolder(nullableOpIfNeeded(op, nullable), normSchema, rest)
      case Schema.Type.ARRAY =>
        val arrayElemSchema = normSchema.getElementType
        val (arrayInnerOps, flatten, remainingField) =
          recursiveArrayInnerOperator(arrayElemSchema, rest)
        val op = ArrayOperator(innerField.pos(), arrayInnerOps, flatten)
        AvroOperatorsHolder(nullableOpIfNeeded(op, nullable), arrayElemSchema, remainingField)
      case Schema.Type.MAP =>
        val (mapKey, mapValueRest) = mapKeyExtractor(rest)
        val mapValueSchema = normSchema.getValueType
        val op = MapOperator(innerField.pos(), mapKey)
        AvroOperatorsHolder(nullableOpIfNeeded(op, nullable), mapValueSchema, mapValueRest)
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

  private def nullableOpIfNeeded(op: BaseOperator, nullable: Boolean): BaseOperator = {
    if (nullable) NullableOperator(op) else op
  }

  private def recursiveArrayInnerOperator(
    innerSchema: Schema, remainingField: Option[String]
  ): (List[BaseOperator], Boolean, Option[String]) = {
    if (remainingField.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroOperators(remainingField.get, innerSchema)
      // innerOps represents the list of operators to be applied to each element in an array
      val innerOps = recursiveResult.map(_.ops)
      // flattenFlag is true if one of the internal operation types is an array-based operation
      val flattenFlag = innerOps.exists(_.isInstanceOf[ArrayOperator])
      // remainingPath represents the remaining user provided string to be traversed
      val remainingPath = recursiveResult.lastOption.flatMap(_.rest)
      (innerOps, flattenFlag, remainingPath)
    } else {
      (List(NoopOperator()), false, None)
    }
  }

  private def mapKeyExtractor(remainingField: Option[String]): (Option[String], Option[String]) = {
    val mapKeyRegex = """^([^.]*)\.?(.*)$""".r
    val mapRegexRes = remainingField.map { case mapKeyRegex(key, rest) =>
      (key, if (rest.isEmpty) None else Some(rest))
    }
    (mapRegexRes.map(_._1), mapRegexRes.flatMap(_._2))
  }

  private def validateSchemaAndInput(op: String, schema: Schema): Unit = {
    (op, schema.getType) match {
      case (op, schema) if op == "[]" & schema != Schema.Type.ARRAY =>
        throw new InvalidAvroFieldSpecifiedException(
          "[] filter is applicable to Array schema types")
      case (op, schema) if schema == Schema.Type.ARRAY & op != "[]." & op != "[]" =>
        throw new InvalidAvroFieldSpecifiedException(
          "[] filter is required to be provided for Array schema types")
      case (op, schema) if op.nonEmpty & op != "[]" & op != "." & op != "[]." =>
        throw new InvalidAvroFieldSpecifiedException(
          s"$op filter not supported for schema type $schema")
      case (_, _) =>
    }
  }
}

case class AvroOperatorsHolder(ops: BaseOperator, schema: Schema, rest: Option[String])
