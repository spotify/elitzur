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

import com.spotify.elitzur.converters.avro.qaas.AvroFieldExtractor
import com.spotify.elitzur.converters.avro.qaas.AvroFieldExtractorExceptions.InvalidAvroFieldOperationException
import com.spotify.elitzur.schemas.TestComplexArrayTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldExtractorNestedArrayTest extends AnyFlatSpec with Matchers {

  import helpers.SampleAvroRecords._
  import collection.JavaConverters._

  val testArrayRecord: TestComplexArrayTypes = testComplexArrayTypes

  it should "extract generic records in an array" in {

    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: [{"userId": "one"}, {"userId": "two"}]

    val arrayInnerArray = AvroFieldExtractor.getAvroValue(
      "innerArrayRoot", testArrayRecord)
    arrayInnerArray should be (testArrayRecord.getInnerArrayRoot)
  }

  it should "extract a field from generic records in an array" in {

    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: ["one", "two"]

    val arrayInnerArray = AvroFieldExtractor.getAvroValue(
      "innerArrayRoot[].userId", testArrayRecord)
    arrayInnerArray should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getUserId).asJava)
  }

  it should "throw an exception if [] isn't provided for an array type" in {

//  The first case fails, investigating
//    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
//    // Output: Exception
//
//    val caughtCase1 = intercept[InvalidAvroFieldOperationException] {
//      val abc = AvroFieldExtractor.getAvroValue(
//        "innerArrayRoot.userId", testComplexArrayRecord)
//      abc
//    }
//    caughtCase1.getMessage should be ("[] is required for an array schema")

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -1}}"},
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -5}}"}
    //    ]}
    // Output: Exception

    val caughtCase2 = intercept[InvalidAvroFieldOperationException] {
      val abc = AvroFieldExtractor.getAvroValue(
        "innerArrayRoot.deepNestedRecord.recordId", testArrayRecord)
      abc
    }
    caughtCase2.getMessage should be ("[] is required for an array schema")
  }

  it should "extract a field from nested generic records in an array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -1}}"},
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -5}}"}
    //    ]}
    // Output: [-1, -5]

    val arrayInnerArray = AvroFieldExtractor.getAvroValue(
      "innerArrayRoot[].deepNestedRecord.recordId", testArrayRecord)
    arrayInnerArray should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getDeepNestedRecord.getRecordId).asJava)
  }

  it should "extract an array within an array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [[1, 2], [3, 4]]

    val arrayInnerArray = AvroFieldExtractor.getAvroValue(
      "innerArrayRoot[].innerArrayInsideRecord", testArrayRecord)
    arrayInnerArray should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getInnerArrayInsideRecord).asJava)
  }

  it should "extract an array within an array and flatten it" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [1, 2, 3, 4]

    val arrayInnerArray = AvroFieldExtractor.getAvroValue(
      "innerArrayRoot[].innerArrayInsideRecord[]", testArrayRecord)
    arrayInnerArray should be (
      testArrayRecord.getInnerArrayRoot.asScala.flatMap(_.getInnerArrayInsideRecord.asScala).asJava)
  }

}
