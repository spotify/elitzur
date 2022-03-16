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
import com.spotify.elitzur.helpers._
import com.spotify.elitzur.schemas.{TestAvroArrayTypes, TestAvroTypes}
import com.spotify.elitzur.validators.{BaseCompanion, Validator}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar.mock

class DynamicAccessorValidationHelpers(
  input: Array[(String, BaseCompanion[_, _], Validator[Any])]) {
  // Instantiate implicit metric reporter
  val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  // The following class generates the accessor function based on the user provided input and the
  // Avro schema. It also uses the companion object provided in the input to determine how to
  // validate a given field.
  val dynamicRecordValidator = new DynamicAccessorValidator(
    input, TestAvroTypes.SCHEMA$)(metricsReporter)

  def getFieldParser(input: String, c: BaseCompanion[_, _]): DynamicFieldParser = {
    dynamicRecordValidator.fieldParsers
      .find(x => x.fieldLabel == s"$input:${c.validationType}").get
  }

  def getValidAndInvalidCounts(input: String, c: BaseCompanion[_, _]): (Int, Int) = {
    val parser = getFieldParser(input, c)
    val m = metricsReporter.asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
    val args = (dynamicRecordValidator.className, parser.fieldLabel, parser.fieldValidationType)
    ((m.getValid _).tupled(args), (m.getInvalid _).tupled(args))
  }
}

class DynamicAccessorValidationTest extends AnyFlatSpec with Matchers {
  // Input expected to be in the format below
  val userInput: Array[(String, BaseCompanion[_, _], Validator[Any])] = Array(
    (".inner.playCount",
      NonNegativeLongCompanion,
      implicitly[Validator[NonNegativeLong]].asInstanceOf[Validator[Any]]
    ),
    (".inner.countryCode",
      CountryCompanion,
      implicitly[Validator[CountryCode]].asInstanceOf[Validator[Any]]
    )
  )

  it should "correctly count the valid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = true)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount", NonNegativeLongCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode", CountryCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((1, 0, 1, 0))
  }

  it should "correctly count the invalid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = false)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount", NonNegativeLongCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode", CountryCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((0, 1, 0, 1))
  }

  it should "throw an exception if invalid input is provided" in {
    val invalidUserInput: Array[(String, BaseCompanion[_, _], Validator[Any])] = Array(
      ("not.a.field", mock[BaseCompanion[_, _]], mock[Validator[Any]]),
      (".innerArrayRoot.deepNestedRecord", mock[BaseCompanion[_, _]], mock[Validator[Any]])
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

