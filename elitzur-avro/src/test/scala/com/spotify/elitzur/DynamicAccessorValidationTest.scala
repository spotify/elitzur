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
import com.spotify.elitzur.helpers.DynamicAccessorValidatorTestUtils.TestMetricsReporter
import com.spotify.elitzur.helpers._
import com.spotify.elitzur.schemas.TestAvroTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class DynamicAccessorValidationHelpers(input: Array[(String, DynamicValidationCompanion)]) {
  // Instantiate implicit metric reporter
  val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  // The following class generates the accessor function based on the user provided input and the
  // Avro schema. It also uses the companion object provided in the input to determine how to
  // validate a given field.
  val dynamicRecordValidator = new DynamicAccessorValidator(
    input, TestAvroTypes.SCHEMA$)(metricsReporter)

  def getFieldParser(input: String, c: DynamicValidationCompanion): DynamicFieldParser = {
    dynamicRecordValidator.fieldParsers
      .find(x => x.label == s"$input:${c.validatorIdentifier}").get
  }

  def getValidAndInvalidCounts(input: String, c: DynamicValidationCompanion): (Int, Int) = {
    val parser = getFieldParser(input, c)
    val m = metricsReporter.asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
    val args = (dynamicRecordValidator.className, parser.label, parser.companion
      .validatorIdentifier)
    ((m.getValid _).tupled(args), (m.getInvalid _).tupled(args))
  }
}

class DynamicAccessorValidationTest extends AnyFlatSpec with Matchers {

  it should "correctly count the valid fields" in {
    // Input expected to be in the format below
    val userInput: Array[(String, DynamicValidationCompanion)] = Array(
      (".inner.playCount", DynamicAccessorValidatorTestUtils.NonNegativeLongDynamicCompanion),
      (".inner.countryCode", DynamicAccessorValidatorTestUtils.CountryCodeDynamicCompanion)
    )
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = true)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount", DynamicAccessorValidatorTestUtils.NonNegativeLongDynamicCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode", DynamicAccessorValidatorTestUtils.CountryCodeDynamicCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((1, 0, 1, 0))
  }

  it should "correctly count the invalid fields" in {
    // Input expected to be in the format below
    val userInput: Array[(String, DynamicValidationCompanion)] = Array(
      (".inner.playCount", DynamicAccessorValidatorTestUtils.NonNegativeLongDynamicCompanion),
      (".inner.countryCode", DynamicAccessorValidatorTestUtils.CountryCodeDynamicCompanion)
    )
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = false)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount", DynamicAccessorValidatorTestUtils.NonNegativeLongDynamicCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode", DynamicAccessorValidatorTestUtils.CountryCodeDynamicCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((0, 1, 0, 1))
  }

  it should "throw an exception if invalid input is provided" in {
    val invalidUserInput: Array[(String, DynamicValidationCompanion)] = Array(
      ("not.a.field", mock[DynamicValidationCompanion]),
    )

    val thrown = intercept[Exception] {
      new DynamicAccessorValidator(invalidUserInput, TestAvroTypes.SCHEMA$)(
        mock[TestMetricsReporter])
    }

    thrown.getMessage should be (
      "Invalid field not.a.field for schema org.apache.avro.Schema$RecordSchema")
  }

}

