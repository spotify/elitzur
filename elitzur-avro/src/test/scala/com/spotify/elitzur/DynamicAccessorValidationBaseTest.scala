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

import com.spotify.elitzur.converters.avro.dynamic._
import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroAccessorException
import com.spotify.elitzur.helpers.DynamicAccessorValidatorTestUtils.TestMetricsReporter
import com.spotify.elitzur.helpers._
import com.spotify.elitzur.schemas.{TestAvroArrayTypes, TestAvroTypes}
import com.spotify.elitzur.validators.{BaseCompanion, Validator}
import com.spotify.elitzur.{
  CountryCodeTesting,
  NonNegativeLongTesting,
  CountryCodeTestingCompanion,
  NonNegativeLongTestingCompanion
}

import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class DynamicAccessorValidationBaseTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  // Input expected to be in the format below
  implicit val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  override def afterEach(): Unit = {
    metricsReporter.asInstanceOf[TestMetricsReporter].cleanSlate()
  }

  val userInput: Array[RecordValidatorProperty] = Array(
    RecordValidatorProperty(
      ".inner.playCount",
      NonNegativeLongTestingCompanion,
      implicitly[Validator[NonNegativeLongTesting]].asInstanceOf[Validator[Any]],
      BaseElitzurMode
    ),
    RecordValidatorProperty(
      ".inner.countryCode",
      CountryCodeTestingCompanion,
      implicitly[Validator[CountryCodeTesting]].asInstanceOf[Validator[Any]],
      BaseElitzurMode
    )
  )

  it should "correctly count the valid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput, TestAvroTypes.SCHEMA$)

    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = true)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount", NonNegativeLongTestingCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode", CountryCodeTestingCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((1, 0, 1, 0))
  }

  it should "correctly count the invalid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput, TestAvroTypes.SCHEMA$)

    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = false)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount", NonNegativeLongTestingCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode", CountryCodeTestingCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((0, 1, 0, 1))
  }

  it should "throw an exception if invalid input is provided" in {
    val invalidUserInput: Array[RecordValidatorProperty] = Array(
      RecordValidatorProperty(
        "not.a.field", mock[BaseCompanion[_, _]], mock[Validator[Any]]),
      RecordValidatorProperty(
        ".innerArrayRoot.deepNestedRecord", mock[BaseCompanion[_, _]], mock[Validator[Any]])
    )

    val thrown = intercept[Exception] {
      new DynamicAccessorValidator(invalidUserInput, TestAvroArrayTypes.SCHEMA$)(
        mock[MetricsReporter])
    }

    List(
      AvroAccessorException.MISSING_TOKEN,
      AvroAccessorException.MISSING_ARRAY_TOKEN
    ).foldLeft(true)((a, c) => thrown.getMessage.contains(c) && a) should be (true)
  }

}

