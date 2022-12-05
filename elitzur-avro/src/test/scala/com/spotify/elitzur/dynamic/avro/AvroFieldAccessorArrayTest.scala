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
import com.spotify.elitzur.helpers.SampleAvroRecords.testAvroArrayTypes
import com.spotify.elitzur.schemas.TestAvroArrayTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import collection.JavaConverters._

class AvroFieldAccessorArrayTest extends AnyFlatSpec with Matchers {
  val testArrayRecord: TestAvroArrayTypes = testAvroArrayTypes

  it should "extract generic records in an array" in {
    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: [{"userId": "one"}, {"userId": "two"}]
    val fn = new FieldAccessor(testArrayRecord.getSchema)
      .getFieldAccessor(".innerArrayRoot[]")

    fn.accessorFns(testArrayRecord) should be (testArrayRecord.getInnerArrayRoot)
  }

  it should "extract a field from generic records in an array" in {
    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: ["one", "two"]
    val fn = new FieldAccessor(testArrayRecord.getSchema)
      .getFieldAccessor(".innerArrayRoot[].userId")

    fn.accessorFns(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getUserId).asJava)
  }

  it should "extract a field from nested generic records in an array" in {
    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -1}}"},
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -5}}"}
    //    ]}
    // Output: [-1, -5]
    val fn = new FieldAccessor(testArrayRecord.getSchema)
      .getFieldAccessor(".innerArrayRoot[].deepNestedRecord.recordId")

    fn.accessorFns(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getDeepNestedRecord.getRecordId).asJava)
  }

  it should "flatten the resulting array" in {
    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [1, 2, 3, 4]
    val fn = new FieldAccessor(testArrayRecord.getSchema)
      .getFieldAccessor(".innerArrayRoot[].innerArrayInsideRecord[]")

    fn.accessorFns(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala
        .flatMap(_.getInnerArrayInsideRecord.asScala).asJava
    )
  }

  it should "not flatten the resulting array" in {
    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [[1, 2], [3, 4]]
    val fn = new FieldAccessor(testArrayRecord.getSchema)
      .getFieldAccessor(".innerArrayRoot[].innerArrayInsideRecord")

    fn.accessorFns(testArrayRecord) should be(
      testArrayRecord.getInnerArrayRoot.asScala
        .map(_.getInnerArrayInsideRecord).asJava
    )
  }

  it should "flatten resulting array with a nullable field in the path" in {
    // Input: {"innerArrayRoot": [
    //    {"deeperArrayNestedRecord": {"DeeperArray": [1, 2]}},
    //    {"deeperArrayNestedRecord": {"DeeperArray": [3, 4]}}
    //    ]}
    // Output: [1, 2, 3, 4]
    val fn = new FieldAccessor(testArrayRecord.getSchema)
      .getFieldAccessor(".innerArrayRoot[].deeperArrayNestedRecord.DeeperArray[]")

    fn.accessorFns(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala
        .flatMap(_.getDeeperArrayNestedRecord.getDeeperArray.asScala).asJava
    )
  }
}
