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
import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroAccessorUtil.mapToAccessors
import org.apache.avro.Schema

import java.{util => ju}

trait BaseAccessorLogic {
  val accessor: BaseAccessor
  val avroOp: AvroAccessorContainer
}

class IndexAccessorLogic(schema: Schema, fieldTokens: AvroFieldTokens) extends BaseAccessorLogic {
  override val accessor: BaseAccessor = IndexAccessor(fieldTokens.field)
  override val avroOp: AvroAccessorContainer =
    AvroAccessorContainer(accessor, schema, fieldTokens.rest)
}

class NullableAccessorLogic(
  schema: Schema, fieldTokens: AvroFieldTokens) extends BaseAccessorLogic {
  private final val nullableToken = "?"

  val nonNullSchema: Schema = getNonNullSchema(schema)
  val headAccessor: AvroAccessorContainer = mapToAccessors(nonNullSchema, fieldTokens)
  override val accessor: BaseAccessor =
    getNullableAccessor(nonNullSchema, fieldTokens.field, headAccessor)
  override val avroOp: AvroAccessorContainer = AvroAccessorContainer(accessor, nonNullSchema, None)

  private def getNullableAccessor(
    innerSchema: Schema, field: String, headAccessor: AvroAccessorContainer
  ): NullableAccessor = {
    if (headAccessor.rest.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroAccessors(headAccessor.rest.get, innerSchema)
      // innerOps represents the list of all accessors to be applied if the avro obj is not null
      val innerOps = (headAccessor +: recursiveResult).map(_.ops)
      NullableAccessor(field, innerOps)
    } else {
      NullableAccessor(field, List(headAccessor.ops))
    }
  }

  private def getNonNullSchema(schema: Schema): Schema = {
    val nonNullSchemas: ju.ArrayList[Schema] = new ju.ArrayList[Schema]
    schema.getTypes.forEach(s => if (s.getType != Schema.Type.NULL) {nonNullSchemas.add(s)})
    if (nonNullSchemas.size > 1) {
      throw new InvalidDynamicFieldException(INVALID_UNION_SCHEMA)
    }
    nonNullSchemas.get(0)
  }
}

class ArrayAccessorLogic(
  arrayElemSchema: Schema, fieldTokens: AvroFieldTokens) extends BaseAccessorLogic {
  private final val arrayToken = "[]"

  override val accessor: BaseAccessor = getArrayAccessor(arrayElemSchema, fieldTokens)
  override val avroOp: AvroAccessorContainer =
    AvroAccessorContainer(accessor, arrayElemSchema, None)

  private def getArrayAccessor(innerSchema: Schema, fieldTokens: AvroFieldTokens): BaseAccessor = {
    if (fieldTokens.rest.isDefined) {
      if (!fieldTokens.op.contains(arrayToken)) {
        throw new InvalidDynamicFieldException(MISSING_ARRAY_TOKEN)
      }
      val recursiveResult = AvroObjMapper.getAvroAccessors(fieldTokens.rest.get, innerSchema)
      // innerOps represents the list of accessors to be applied to each element in an array
      val innerOps = recursiveResult.map(_.ops)
      // flattenFlag is true if one of the internal operation types is a map based operation
      val flattenFlag = getFlattenFlag(recursiveResult.map(_.ops))
      if (flattenFlag) {
        ArrayFlatmapAccessor(fieldTokens.field, innerOps)
      } else {
        ArrayMapAccessor(fieldTokens.field, innerOps)
      }
    } else {
      val headAccessor: BaseAccessor = mapToAccessors(innerSchema, fieldTokens).ops
      ArrayNoopAccessor(fieldTokens.field, List(headAccessor), fieldTokens.op.contains(arrayToken))
    }
  }

  private def getFlattenFlag(ops: List[BaseAccessor]): Boolean = {
    ops.foldLeft(false)((accBoolean, currAccessor) => {
      val hasArrayAccessor = currAccessor match {
        case a: ArrayNoopAccessor => a.flatten
        case n: NullableAccessor => getFlattenFlag(n.innerOps)
        case _: ArrayMapAccessor | _: ArrayFlatmapAccessor => true
        case _ => false
      }
      accBoolean || hasArrayAccessor
    })
  }
}
