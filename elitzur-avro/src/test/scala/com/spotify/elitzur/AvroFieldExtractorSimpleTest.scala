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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldExtractorSimpleTest extends AnyFlatSpec with Matchers {
  import helpers.SampleAvroRecords._

  def combineFns(fns: List[AvroRecursiveDataHolder]): Any => Any =
    (fns.map(_.ops) :+ NoopAvroObjWrapper()).reduceLeftOption((f, g) => f + g).get.fn

  it should "extract a primitive at the record root level" in {
    val testSimpleAvroRecord = innerNestedSample()
    val fn = combineFns(
      AvroObjMapper.extract("userId", testSimpleAvroRecord.getSchema)
    )
    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getUserId)
  }

  it should "extract an array at the record root level" in {
    val testSimpleAvroRecord = testAvroRecord(2)
    val fn = combineFns(
      AvroObjMapper.extract("arrayLongs", testSimpleAvroRecord.getSchema)
    )
    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getArrayLongs)
  }

  it should "extract a nested record" in {
    val testSimpleAvroRecord = testAvroRecord(2)
    val avroPath = "innerOpt.userId"

    val fns = AvroObjMapper.extract(avroPath, testSimpleAvroRecord.getSchema)
    val fn = combineFns(fns)

    //    fns.map(_.ops) should be (List(GenericRecordOperation(3), GenericRecordOperation(0)))
    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getInnerOpt.getUserId)
  }

  //  it should "extract complex case" in {
  //
  //    val testRecord = testAvroRecord(2)
  //    val fns = AvroFieldExtractorV2.getAvroValue(
  //        "arrayInnerNested[].innerNested.arrayInnerNested[].countryCode[]", testRecord.getSchema)
  //    val fn = combineFns(fns)
  //
  //    val whatisthis = fn(testRecord)
  //    whatisthis
  //  }
}
