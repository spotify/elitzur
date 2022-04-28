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
import com.spotify.elitzur.converters.avro.dynamic.{DynamicAccessorCompanion, DynamicFieldParser}
import com.spotify.elitzur.helpers.DynamicAccessorValidatorTestUtils.TestMetricsReporter
import com.spotify.elitzur.schemas.TestAvroArrayTypes
import com.spotify.ratatool.scalacheck.avroOf
import com.spotify.ratatool.scalacheck._
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import collection.JavaConverters._

class DynamicAccessorValidationArrayTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  import com.spotify.elitzur.helpers._
  import Companions._

  implicit val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  override def afterEach(): Unit = {
    metricsReporter.asInstanceOf[TestMetricsReporter].cleanSlate()
  }

  it should "correctly validate and invalidate elements in a list (Seq)" in {
    val userInput: Array[DynamicFieldParser] = Array(
      new DynamicFieldParser(
        ".arrayLongs:NonNegativeLong",
        new DynamicAccessorCompanion[Long, NonNegativeLong],
        AvroObjMapper.getAvroFun(".arrayLongs", TestAvroArrayTypes.SCHEMA$)
      )
    )

    val testSetUp = new DynamicAccessorValidationHelpers(userInput)
    val validAvroRecord: TestAvroArrayTypes = avroOf[TestAvroArrayTypes].sample.get

    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".arrayLongs:NonNegativeLong", NonNegativeLongCompanion)

    val (expectedValid, expectedInvalid) = validAvroRecord
      .getArrayLongs
      .asScala
      .map(NonNegativeLong(_))
      .partition(_.checkValid)

    (playCountValidCount, playCountInvalidCount) should be(
      (expectedValid.length, expectedInvalid.length))
  }

  it should "correctly validate and invalidate nullable elements in a list (Seq.Option)" in {
    val userInput: Array[DynamicFieldParser] = Array(
      new DynamicFieldParser(
        ".arrayNullableStrings:CountryCode",
        new DynamicAccessorCompanion[String, CountryCode],
        AvroObjMapper.getAvroFun(".arrayNullableStrings", TestAvroArrayTypes.SCHEMA$)
      )
    )

    val testSetUp = new DynamicAccessorValidationHelpers(userInput)
    val validAvroRecord: TestAvroArrayTypes = avroOf[TestAvroArrayTypes]
      .amend(List(
        "SE".asInstanceOf[CharSequence],
        "NYC".asInstanceOf[CharSequence],
        null).asJava)(_.setArrayNullableStrings)
      .sample.get

    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (countryCountValidCount, countryCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".arrayNullableStrings:CountryCode", CountryCompanion)

    (countryCountValidCount, countryCountInvalidCount) should be((2, 1))
  }

}
