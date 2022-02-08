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

package com.spotify.elitzur.helpers

import java.util
import com.spotify.elitzur.schemas._
import com.spotify.ratatool.scalacheck.{RichAvroGen, avroOf}
import org.scalacheck.{Arbitrary, Gen}
import com.spotify.ratatool.scalacheck._

case class OptFieldLong(optFieldLong: Option[Long])
case class OptInnerField(optInnerField: Option[OptFieldLong])

object SampleAvroRecords {

  import collection.JavaConverters._

  def innerNestedSample(isValid: Boolean = true): InnerNestedType = avroOf[InnerNestedType]
    .amend(if (isValid) Gen.const("US") else Gen.const("NYC"))(_.setCountryCode)
    .amend(if (isValid) Gen.posNum[Long] else Gen.negNum[Long])(_.setPlayCount)
    .sample.get

  def innerNestedListSample(elems: Int, isValid: Boolean = true): util.List[InnerNestedType] =
    List.fill(elems)(innerNestedSample(isValid)).asJava

  def innerNestedArrayTypeSample(elems: Int, isValid: Boolean = true): InnerNestedArrayType =
    avroOf[InnerNestedArrayType]
      .amend(Gen.const(innerNestedListSample(elems, isValid)))(_.setArrayInnerNested)
      .sample.get

  def innerNestedWithArrayFieldSample(elems: Int, isValid: Boolean = true)
  : util.List[InnerNestedWithArrayFieldType] =
    Gen.listOfN(2, avroOf[InnerNestedWithArrayFieldType]
      .amend(Gen.const(innerNestedArrayTypeSample(elems, isValid)))(_.setInnerNested))
      .sample.get.asJava

  def testAvroRecord(elems: Int, isValid: Boolean = true): TestAvroArrayTypes =
    TestAvroArrayTypes.newBuilder()
      .setUserAge(0L)
      .setUserFloat(0F)
      .setUserLong(0L)
      .setInnerOpt(innerNestedSample(isValid))
      .setArrayLongs(List(new java.lang.Long(elems)).asJava)
      .setArrayInnerNested(innerNestedWithArrayFieldSample(elems, isValid))
      .build()

  def innerComplexType(isInternalNull: Boolean): InnerComplexType =
    avroOf[InnerComplexType]
      .amend(if (isInternalNull) Gen.const(null) else Gen.const("abc"))(_.setOptString)
      .sample.get

  def testComplexTypeRecord(isNull: Boolean, isInternalNull: Boolean): TestComplexSchemaTypes = {
    avroOf[TestComplexSchemaTypes]
      .amend(if (isNull) Gen.const(null) else innerComplexType(isInternalNull))(_.setOptRecord)
      .sample.get
  }

  def testComplexArrayTypes: TestComplexArrayTypes = {
    val innerArrayInsideRecord = Gen.listOfN[Long](2, Arbitrary.arbitrary[Long])
      .sample.get.map(_.asInstanceOf[java.lang.Long]).asJava
    val innerArrayRecord = avroOf[innerArrayRecord]
      .amend(innerArrayInsideRecord)(_.setInnerArrayInsideRecord)
    def innerArrayRoot: Gen[util.List[innerArrayRecord]] =
      Gen.listOfN(2, innerArrayRecord).map(_.asJava)

    avroOf[TestComplexArrayTypes]
      .amend(innerArrayRoot)(_.setInnerArrayRoot)
      .sample
      .get
  }
}
