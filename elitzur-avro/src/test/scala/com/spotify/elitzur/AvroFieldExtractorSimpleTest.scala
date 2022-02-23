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
import helpers.SampleAvroRecords._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldExtractorSimpleTest extends AnyFlatSpec with Matchers {

  it should "extract a primitive at the record root level" in {
    val testSimpleAvroRecord = innerNestedSample()
    val fn = AvroObjMapper.getAvroFun(".userId", testSimpleAvroRecord.getSchema)

    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getUserId)
  }

  it should "extract an array at the record root level" in {
    val testSimpleAvroRecord = testAvroArrayTypes
    val fn = AvroObjMapper.getAvroFun(".arrayLongs", testSimpleAvroRecord.getSchema)

    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getArrayLongs)
  }

  it should "extract a nested record" in {
    val testSimpleAvroRecord = testAvroTypes
    val fn = AvroObjMapper.getAvroFun(".inner.userId", testSimpleAvroRecord.getSchema)

    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getInner.getUserId)
  }
}
