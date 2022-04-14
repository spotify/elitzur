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

import com.spotify.elitzur.converters.avro.dynamic.{NullableElitzurMode, RecordValidatorProperty}
import com.spotify.elitzur.helpers.DynamicAccessorValidatorTestUtils.TestMetricsReporter
import com.spotify.elitzur.helpers.{
  DynamicAccessorValidationHelpers,
  DynamicAccessorValidatorTestUtils
}
import com.spotify.elitzur.schemas.{InnerComplexType, TestAvroUnionTypes}
import com.spotify.elitzur.validators.Validator
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DynamicAccessorValidationUnionTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  // Input expected to be in the format below
  implicit val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  override def afterEach(): Unit = {
    metricsReporter.asInstanceOf[TestMetricsReporter].cleanSlate()
  }

  val userInput: Array[RecordValidatorProperty] = Array(
    RecordValidatorProperty(
      ".optRecord.optString",
      CountryCodeTestingCompanion,
      implicitly[Validator[Option[CountryCodeTesting]]].asInstanceOf[Validator[Any]],
      NullableElitzurMode
    )
  )

  it should "correctly validate a nullable field if the field exists and is a country code" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput, TestAvroUnionTypes.SCHEMA$)

    val validAvroRecord = TestAvroUnionTypes.newBuilder()
      .setOptRecord(
        InnerComplexType.newBuilder()
          .setOptString("US")
          .setOptRepeatedArray(null).build()
      ).build

    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".optRecord.optString", CountryCodeTestingCompanion)

    (countryCodValidCount, countryCodInvalidCount) should be ((1, 0))
  }

  it should "correctly validate a null value" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput, TestAvroUnionTypes.SCHEMA$)

    val inValidAvroRecord = TestAvroUnionTypes.newBuilder()
      .setOptRecord(
        InnerComplexType.newBuilder()
          .setOptString(null)
          .setOptRepeatedArray(null)
          .build()
      ).build

    testSetUp.dynamicRecordValidator.validateRecord(inValidAvroRecord)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".optRecord.optString", CountryCodeTestingCompanion)

    (countryCodValidCount, countryCodInvalidCount) should be ((1, 0))
  }
}

