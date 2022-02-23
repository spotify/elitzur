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

trait BaseFilterLogic {
  val filter: BaseFilter
  val avroOp: AvroOperatorsHolder
}

class IndexFilterLogic(schema: Schema, pos: Int, rest: Option[String]) extends BaseFilterLogic {
  override val filter: BaseFilter = IndexFilter(pos)
  override val avroOp: AvroOperatorsHolder = AvroOperatorsHolder(filter, schema, rest)
}

class NullableFilterLogic(
  schema: Schema, pos: Int, firstFilter: AvroOperatorsHolder
  ) extends BaseFilterLogic {
  private final val nullableToken = "?"

  override val filter: BaseFilter = getNullableFilter(schema, pos, firstFilter)
  override val avroOp: AvroOperatorsHolder = AvroOperatorsHolder(filter, schema, None)

  private def getNullableFilter(
    innerSchema: Schema, pos: Int, firstFilter: AvroOperatorsHolder
  ): NullableFilter = {
    if (firstFilter.rest.isDefined) {
      val recursiveResult = AvroObjMapper.getAvroFilters(firstFilter.rest.get, innerSchema)
      // innerOps represents the list of all filters to be applied if the avro obj is not null
      val innerOps = AvroObjMapper.combineFns((firstFilter +: recursiveResult).map(_.ops))
      NullableFilter(pos, innerOps)
    } else {
      NullableFilter(pos, firstFilter.ops.fn)
    }
  }
}

class ArrayFilterLogic(
  arrayElemSchema: Schema, pos: Int, rest: Option[String], op: Option[String]
  ) extends BaseFilterLogic {
  private final val arrayToken = "[]"

  override val filter: BaseFilter = getArrayFilter(arrayElemSchema, rest, op, pos)
  override val avroOp: AvroOperatorsHolder = AvroOperatorsHolder(filter, arrayElemSchema, None)

  private def getArrayFilter(
    innerSchema: Schema, remainingField: Option[String], opToken: Option[String], pos: Int
  ): ArrayFilter = {
    if (remainingField.isDefined) {
      if (!opToken.contains(arrayToken)) throw new InvalidDynamicFieldException(MISSING_ARRAY_TOKEN)
      val recursiveResult = AvroObjMapper.getAvroFilters(remainingField.get, innerSchema)
      // innerOps represents the list of filters to be applied to each element in an array
      val innerOps = AvroObjMapper.combineFns(recursiveResult.map(_.ops))
      // flattenFlag is true if one of the internal operation types is a flatmap based operation
      val flattenFlag = recursiveResult
        .filter(_.ops.isInstanceOf[ArrayFilter])
        .exists(_.ops.asInstanceOf[ArrayFilter].flatten)
      ArrayFilter(pos, innerOps, flattenFlag, isLastArray = false)
    } else {
      ArrayFilter(pos, NoopFilter().fn, opToken.contains(arrayToken), isLastArray = true)
    }

  }
}