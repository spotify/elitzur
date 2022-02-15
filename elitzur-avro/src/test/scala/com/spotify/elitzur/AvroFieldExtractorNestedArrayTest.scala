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

import com.spotify.elitzur.converters.avro.qaas.{AvroObjMapper, AvroRecursiveDataHolder}
import com.spotify.elitzur.converters.avro.qaas.utils.NoopAvroObjWrapper
import com.spotify.elitzur.schemas.TestComplexArrayTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldExtractorNestedArrayTest extends AnyFlatSpec with Matchers {

  import helpers.SampleAvroRecords._
  import collection.JavaConverters._

  val testArrayRecord: TestComplexArrayTypes = testComplexArrayTypes
  def combineFns(fns: List[AvroRecursiveDataHolder]): Any => Any =
    ((fns).map(_.ops) :+ NoopAvroObjWrapper()).reduceLeftOption((f, g) => f + g).get.fn

  it should "extract generic records in an array" in {

    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: [{"userId": "one"}, {"userId": "two"}]
    val fns = AvroObjMapper.extract("innerArrayRoot", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (testArrayRecord.getInnerArrayRoot)
  }

  it should "extract a field from generic records in an array" in {

    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: ["one", "two"]
    val fns = AvroObjMapper.extract("innerArrayRoot[].userId",
      testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getUserId).asJava)
  }

  it should "extract a field from nested generic records in an array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -1}}"},
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -5}}"}
    //    ]}
    // Output: [-1, -5]
    val fns = AvroObjMapper.extract(
      "innerArrayRoot[].deepNestedRecord.recordId", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getDeepNestedRecord.getRecordId).asJava)
  }

  it should "extract an array within an array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [[1, 2], [3, 4]]

    val fns = AvroObjMapper.extract(
      "innerArrayRoot[].innerArrayInsideRecord", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getInnerArrayInsideRecord).asJava)
  }

  it should "flatten the resulting array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [[1, 2, 3, 4]]

    val fns = AvroObjMapper.extract(
      "innerArrayRoot[].innerArrayInsideRecord[]", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.flatMap(_.getInnerArrayInsideRecord.asScala).asJava)
  }

}
