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

import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroAccessorException._
import org.apache.avro.Schema

import java.{util => ju}
import scala.annotation.tailrec
import scala.collection.mutable

object AvroObjMapper {
  private val mapToAvroFun: mutable.Map[String, Any => Any] = mutable.Map.empty[String, Any => Any]

  def getAvroFun(avroFieldPath: String, schema: Schema): Any => Any = {
    if (!mapToAvroFun.contains(avroFieldPath)) {
      val avroOperators = getAvroAccessors(avroFieldPath, schema).map(_.ops)
      mapToAvroFun += (avroFieldPath -> combineFns(avroOperators))
    }
    mapToAvroFun(avroFieldPath)
  }

  private val mapToAvroFunWithSchema: mutable.Map[String, Accessor] =
    mutable.Map.empty[String, Accessor]

  def getAvroFunWithSchema(avroFieldPath: String, schema: Schema): Accessor = {
    if (!mapToAvroFunWithSchema.contains(avroFieldPath)) {
      val accessor = Accessor(getAvroAccessors(avroFieldPath, schema))
      mapToAvroFunWithSchema += (avroFieldPath -> accessor)
    }
    mapToAvroFunWithSchema(avroFieldPath)
  }

  @tailrec
  private[dsl] def getAvroAccessors(
    path: String,
    avroSchema: Schema,
    accAvroOperators: List[AvroAccessorContainer] = List.empty[AvroAccessorContainer]
  ): List[AvroAccessorContainer] = {
    val thisAvroOp = AvroAccessorUtil.mapToAccessors(path, avroSchema)
    val appendedAvroOp = accAvroOperators :+ thisAvroOp
    thisAvroOp.rest match {
      case Some(remainingPath) => getAvroAccessors(remainingPath, thisAvroOp.schema, appendedAvroOp)
      case _ => appendedAvroOp
    }
  }

  private[dsl] def combineFns(fns: List[BaseAccessor]): Any => Any =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
}

object AvroAccessorUtil {
  private val PRIMITIVES: ju.EnumSet[Schema.Type] =
    ju.EnumSet.complementOf(ju.EnumSet.of(Schema.Type.ARRAY, Schema.Type.MAP, Schema.Type.UNION))

  def mapToAccessors(path: String, schema: Schema): AvroAccessorContainer = {
    val fieldTokens = pathToTokens(path)
    val fieldSchema = schema.getField(fieldTokens.field)

    mapToAccessors(fieldSchema.schema, fieldTokens)
  }

  def mapToAccessors(fieldSchema: Schema, fieldTokens: AvroFieldTokens): AvroAccessorContainer = {
    fieldSchema.getType match {
      case _schema if PRIMITIVES.contains(_schema) =>
        new IndexAccessorLogic(fieldSchema, fieldTokens).avroOp
      case Schema.Type.ARRAY =>
        new ArrayAccessorLogic(fieldSchema.getElementType, fieldTokens).avroOp
      case Schema.Type.UNION =>
        new NullableAccessorLogic(fieldSchema, fieldTokens).avroOp
      case Schema.Type.MAP => throw new InvalidDynamicFieldException(UNSUPPORTED_MAP_SCHEMA)
    }
  }

  private def pathToTokens(path: String): AvroFieldTokens = {
    def strToOpt(str: String) :Option[String] = if (str.nonEmpty) Some(str) else None
    val token = '.'
    if (path.headOption.contains(token)) {
      val (fieldOps, rest) = path.drop(1).span(_ != token)
      val (field, op) = fieldOps.span(char => char.isLetterOrDigit || char == '_')
      AvroFieldTokens(field, strToOpt(op), strToOpt(rest))
    } else {
      throw new InvalidDynamicFieldException(MISSING_TOKEN)
    }
  }
}

case class Accessor(accessors: List[AvroAccessorContainer]) {
  val accessorFn: Any => Any = combineFns(accessors.map(_.ops))

  val schema: Schema = accessors.map(_.schema).lastOption.get

  val isNullable: Boolean = hasNullable(accessors.map(_.ops))

  val isArray: Boolean = hasArray(accessors.map(_.ops))

  private[dsl] def hasArray(ops: List[BaseAccessor]): Boolean = {
    ops.foldLeft(false)((accBoolean, currAccessor) => {
      val hasArrayAccessor = currAccessor match {
        case n: NullableAccessor => hasArray(n.innerOps)
        case _: ArrayMapAccessor | _: ArrayFlatmapAccessor | _: ArrayNoopAccessor  => true
        case _ => false
      }
      accBoolean || hasArrayAccessor
    })
  }

  private[dsl] def hasNullable(ops: List[BaseAccessor]): Boolean = {
    ops.foldLeft(false)((accBoolean, currAccessor) => {
      val hasNullableAccessor = currAccessor match {
        case _: ArrayNoopAccessor => false
        case _: NullableAccessor => true
        case a: ArrayMapAccessor => hasNullable(a.innerOps)
        case a: ArrayFlatmapAccessor => hasNullable(a.innerOps)
        case _ => false
      }
      accBoolean || hasNullableAccessor
    })
  }

  private[dsl] def combineFns(fns: List[BaseAccessor]): Any => Any =
    fns.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)
}

case class AvroAccessorContainer(ops: BaseAccessor, schema: Schema, rest: Option[String])

case class AvroFieldTokens(field: String, op: Option[String], rest: Option[String])
