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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldExtractorSimpleTest extends AnyFlatSpec with Matchers {
  import helpers.SampleAvroRecords._

  it should "extract a primitive at the record root level" in {
    val testSimpleAvroRecord = innerNestedSample()
    val userId = AvroFieldExtractor.getAvroValue("userId", testSimpleAvroRecord)
    userId should be (testSimpleAvroRecord.getUserId)
  }

  it should "extract an array at the record root level" in {
    val testSimpleAvroRecord = testAvroRecord(2)
    val arrayOfLong = AvroFieldExtractor.getAvroValue("arrayLongs", testSimpleAvroRecord)
    arrayOfLong should be (testSimpleAvroRecord.getArrayLongs)
  }

  it should "throw an exception if a field is being retrieved from a primitive schema type" in {
    val testSimpleAvroRecord = innerNestedSample()
    val caught = intercept[InvalidAvroFieldOperationException] {
      AvroFieldExtractor.getAvroValue("userId.cannotBeAField", testSimpleAvroRecord)
    }
    caught.getMessage should be ("Avro field cannot be retrieved from a primitive schema type")
  }

  it should "throw an exception for incorrect field name" in {
    val testSimpleAvroRecord = innerNestedSample()
    // NullPointerException is meaningless here, custom exception message should be returned.
    // This is a reminder to do so.
    assertThrows[java.lang.NullPointerException] {
      AvroFieldExtractor.getAvroValue("notAField", testSimpleAvroRecord)
    }
    //    val abc = testAvroRecord(2)
    //    val res2 = AvroFieldExtractor.getAvroValue(
    //      "arrayInnerNested.innerNested.arrayInnerNested.countryCode", abc)
    //    res2
  }
}
