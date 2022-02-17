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

package com.spotify.elitzur

import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroObjMapper
import com.spotify.elitzur.schemas.{MapValueGenericRecord, TestComplexMapTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.{util => ju}

class AvroFieldExtractorMapTest extends AnyFlatSpec with Matchers {
  val mapPrimitiveValue: ju.Map[CharSequence, CharSequence] = new ju.HashMap()
  mapPrimitiveValue.put("a", "bc")
  mapPrimitiveValue.put("1", "23")

  val mapGenericRecord: ju.Map[CharSequence, MapValueGenericRecord] = new ju.HashMap()
  mapGenericRecord.put("xyz", MapValueGenericRecord.newBuilder().setTrackId("track").build())

  val simpleMapAvroRecord: TestComplexMapTypes = TestComplexMapTypes
    .newBuilder()
    .setInnerMapRoot(mapPrimitiveValue)
    .setInnerMapRootWithGenericRecord(mapGenericRecord)
    .build

  it should "extract an map" in {
    val fn = AvroObjMapper.getAvroFun("innerMapRoot.a", simpleMapAvroRecord.getSchema)

    fn(simpleMapAvroRecord) should be (simpleMapAvroRecord.getInnerMapRoot.get("a"))
  }

  it should "empty map key" in {
    val fn = AvroObjMapper.getAvroFun("innerMapRoot", simpleMapAvroRecord.getSchema)

    fn(simpleMapAvroRecord) should be (simpleMapAvroRecord.getInnerMapRoot)
  }

  it should "map with generic map values" in {
    val fn = AvroObjMapper
      .getAvroFun("innerMapRootWithGenericRecord.xyz.trackId", simpleMapAvroRecord.getSchema)

    fn(simpleMapAvroRecord) should be (
      simpleMapAvroRecord.getInnerMapRootWithGenericRecord.get("xyz").getTrackId)
  }
}
