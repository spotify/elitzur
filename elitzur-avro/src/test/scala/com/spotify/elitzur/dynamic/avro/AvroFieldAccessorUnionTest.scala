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
package com.spotify.elitzur.dynamic.avro

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.FieldAccessor
import com.spotify.elitzur.helpers.SampleAvroRecords.innerNestedSample
import com.spotify.elitzur.schemas.{InnerComplexType, TestAvroUnionTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import collection.JavaConverters._

class AvroFieldAccessorUnionTest extends AnyFlatSpec with Matchers {

  it should "extract a null from an Union schema type" in {
    // Input: {"optRecord": null}
    // Output: null
    val fn = new FieldAccessor(TestAvroUnionTypes.SCHEMA$)
      .getFieldAccessor(".optRecord.optString")
    val testNullRecord = TestAvroUnionTypes.newBuilder().setOptRecord(null).build

    fn.accessorFns(testNullRecord) should be (testNullRecord.getOptRecord)
  }

  it should "extract a null from a nested Union Avro schema type" in {
    // Input: {"optRecord": {"optString": null}}
    // Output: null
    val fn = new FieldAccessor(TestAvroUnionTypes.SCHEMA$)
      .getFieldAccessor(".optRecord.optString")
    val testInnerNullRecord = TestAvroUnionTypes.newBuilder()
      .setOptRecord(
        InnerComplexType.newBuilder()
          .setOptString(null)
          .setOptRepeatedArray(null)
          .build()
      ).build

    fn.accessorFns(testInnerNullRecord) should be (testInnerNullRecord.getOptRecord.getOptString)
  }

  it should "extract a primitive from a Union Avro schema type" in {
    // Input: {"optRecord": {"optString": "abc"}}
    // Output: "abc"
    val fn = new FieldAccessor(TestAvroUnionTypes.SCHEMA$)
      .getFieldAccessor(".optRecord.optString")
    val testInnerNonNullRecord = TestAvroUnionTypes.newBuilder()
      .setOptRecord(
        InnerComplexType.newBuilder()
          .setOptString("abc")
          .setOptRepeatedArray(null).build()
      ).build

    fn.accessorFns(testInnerNonNullRecord) should be
      (testInnerNonNullRecord.getOptRecord.getOptString)
  }

  it should "return null if child schema is non-nullable" in {
    // Input: {"optRecord": null}
    // Output: "null"
    val fnNonNull = new FieldAccessor(TestAvroUnionTypes.SCHEMA$)
      .getFieldAccessor(".optRecord.nonOptString")
    val testNullRecord = TestAvroUnionTypes.newBuilder().setOptRecord(null).build

    fnNonNull.accessorFns(testNullRecord) should be (testNullRecord.getOptRecord)
  }

  it should "return the elements of an array if array is not null" in {
    // Input: {"optRecord": {"optRepeatedArray": [{"userId": "a", "countryCode": "US"}]}}
    // Output: "a"
    val fnArrayNull = new FieldAccessor(TestAvroUnionTypes.SCHEMA$)
      .getFieldAccessor(".optRecord.optRepeatedArray[].userId")
    val testInnerNonNullRecord = TestAvroUnionTypes.newBuilder()
      .setOptRecord(
        InnerComplexType.newBuilder()
          .setOptString(null)
          .setOptRepeatedArray(List(innerNestedSample()).asJava).build()
      ).build

    fnArrayNull.accessorFns(testInnerNonNullRecord) should be (
      testInnerNonNullRecord.getOptRecord.getOptRepeatedArray.asScala.map(_.getUserId).asJava)
  }

  it should "return null if array is null" in {
    // Input: {"optRecord": {"optRepeatedArray": null}}
    // Output: null
    val fnArrayNull = new FieldAccessor(TestAvroUnionTypes.SCHEMA$)
      .getFieldAccessor(".optRecord.optRepeatedArray[].userId")
    val testInnerNullRecord = TestAvroUnionTypes.newBuilder()
      .setOptRecord(
        InnerComplexType.newBuilder()
          .setOptString(null)
          .setOptRepeatedArray(null).build()).build

    fnArrayNull.accessorFns(testInnerNullRecord) should be (
      testInnerNullRecord.getOptRecord.getOptRepeatedArray)
  }

}
