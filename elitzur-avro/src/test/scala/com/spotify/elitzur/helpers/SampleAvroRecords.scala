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
import collection.JavaConverters._

case class OptFieldLong(optFieldLong: Option[Long])
case class OptInnerField(optInnerField: Option[OptFieldLong])

object SampleAvroRecords {

  def innerNestedSample(isValid: Boolean = true): InnerNestedType = avroOf[InnerNestedType]
    .amend(if (isValid) Gen.const("US") else Gen.const("NYC"))(_.setCountryCode)
    .amend(if (isValid) Gen.posNum[Long] else Gen.negNum[Long])(_.setPlayCount)
    .sample.get

  def testAvroTypes: TestAvroTypes = avroOf[TestAvroTypes]
    .amend(innerNestedSample())(_.setInner)
    .sample.get

  def testAvroArrayTypes: TestAvroArrayTypes = {
    def innerArrayInsideRecord = Gen.listOfN[Long](2, Arbitrary.arbitrary[Long])
      .sample.get.map(_.asInstanceOf[java.lang.Long]).asJava
    def innerArrayRecord = avroOf[innerArrayRecord]
      .amend(innerArrayInsideRecord)(_.setInnerArrayInsideRecord)
    def innerArrayRoot: Gen[util.List[innerArrayRecord]] =
      Gen.listOfN(2, innerArrayRecord).map(_.asJava)
    avroOf[TestAvroArrayTypes]
      .amend(innerArrayRoot)(_.setInnerArrayRoot)
      .sample
      .get
  }
}
