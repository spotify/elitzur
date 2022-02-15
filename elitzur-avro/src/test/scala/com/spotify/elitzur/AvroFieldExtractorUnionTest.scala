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

import com.spotify.elitzur.converters.avro.qaas.utils.NoopAvroObjWrapper
import com.spotify.elitzur.converters.avro.qaas.{ AvroObjMapper, AvroRecursiveDataHolder}
import com.spotify.elitzur.schemas.{InnerComplexType, TestComplexSchemaTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldExtractorUnionTest extends AnyFlatSpec with Matchers {
  def combineFns(fns: List[AvroRecursiveDataHolder]): Any => Any =
    ((fns).map(_.ops) :+ NoopAvroObjWrapper()).reduceLeftOption((f, g) => f + g).get.fn

  it should "extract a null from an Union schema type v2" in {
    // Input: {"optRecord": null}
    // Output: null
    val testNullRecord = TestComplexSchemaTypes.newBuilder().setOptRecord(null).build
    val avroPath = "optRecord.optString"

    val fns = AvroObjMapper.extract(avroPath, testNullRecord.getSchema)
    val fn = combineFns(fns)

    //    val expectedFns: List[OperationBase] = List[OperationBase](
    //      GenericRecordOperation(0), UnionNullOperation(GenericRecordOperation(0)))
    //
    //    fns.map(_.ops) should be (expectedFns)
    fn(testNullRecord) should be (testNullRecord.getOptRecord)
  }

  it should "extract a null from a nested Union Avro schema type v2" in {
    // Input: {"optRecord": {"optString": null}}
    // Output: null
    val testInnerNullRecord = TestComplexSchemaTypes.newBuilder()
      .setOptRecord(InnerComplexType.newBuilder().setOptString(null).build).build
    val avroPath = "optRecord.optString"

    val fns = AvroObjMapper.extract(avroPath, testInnerNullRecord.getSchema)
    val fn = combineFns(fns)

    //    val expectedFns: List[OperationBase] = List[OperationBase](
    //      GenericRecordOperation(0), UnionNullOperation(GenericRecordOperation(0)))
    //
    //    fns.map(_.ops) should be (expectedFns)
    fn(testInnerNullRecord) should be (testInnerNullRecord.getOptRecord.getOptString)
  }

  it should "extract a primitive from a Union Avro schema type v2" in {
    // Input: {"optRecord": {"optString": "abc"}}
    // Output: "abc"
    val testInnerNonNullRecord = TestComplexSchemaTypes.newBuilder()
      .setOptRecord(InnerComplexType.newBuilder().setOptString("abc").build).build
    val avroPath = "optRecord.optString"

    val fns = AvroObjMapper.extract(avroPath, testInnerNonNullRecord.getSchema)
    val fn = combineFns(fns)

    //    val expectedFns: List[OperationBase] = List[OperationBase](
    //      GenericRecordOperation(0), UnionNullOperation(GenericRecordOperation(0)))
    //
    //    fns.map(_.ops) should be (expectedFns)
    fn(testInnerNonNullRecord) should be (testInnerNonNullRecord.getOptRecord.getOptString)
  }
}
