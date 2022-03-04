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

import com.spotify.elitzur.converters.avro.dynamic.{
  DynamicAccessorValidator,
  DynamicFieldParser,
  DynamicValidationCompanion
}
import com.spotify.elitzur.helpers._
import com.spotify.elitzur.schemas.TestAvroTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class DynamicAccessorValidationTest extends AnyFlatSpec with Matchers {
  // Input expected to be in the format below
  val userInput: Array[(String, DynamicValidationCompanion)] = Array(
    (".inner.playCount", DynamicAccessorValidatorTestUtils.NonNegativeLongDynamicCompanion),
    (".inner.countryCode", DynamicAccessorValidatorTestUtils.CountryCodeDynamicCompanion)
  )

  // Instantiate implicit metric reporter
  implicit val metricsReporter: MetricsReporter = DynamicAccessorValidatorTestUtils.metricsReporter()

  // The following class generates the accessor function based on the user provided input and the
  // Avro schema. It also uses the companion object provided in the input to determine how to
  // validate a given field.
  val dynamicRecordValidator = new DynamicAccessorValidator(userInput, TestAvroTypes.SCHEMA$)

  // Extracting individual field parsers for accessing the valid/invalid counts in metricsReporter
  val playCountFieldParser: DynamicFieldParser = dynamicRecordValidator.fieldParsers
    .find(x => x.label == ".inner.playCount:NonNegativeLong").get
  val userIdFieldParser: DynamicFieldParser = dynamicRecordValidator.fieldParsers
    .find(x => x.label == ".inner.countryCode:CountryCode").get

  it should "correctly count the valid fields" in {
    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = true)

    dynamicRecordValidator.validateRecord(validAvroRecord)

    val validPlayCountValueCount = metricsReporter
      .asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
      .getValid(
        dynamicRecordValidator.className,
        playCountFieldParser.label,
        playCountFieldParser.companion.validatorIdentifier
      )

    val validUserIdCount = metricsReporter
      .asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
      .getValid(
        dynamicRecordValidator.className,
        userIdFieldParser.label,
        userIdFieldParser.companion.validatorIdentifier
      )

    (validPlayCountValueCount, validUserIdCount) should be ((1, 1))
  }

  it should "correctly count the invalid fields" in {
    val validAvroRecord = helpers.SampleAvroRecords.testAvroTypes(isValid = false)

    dynamicRecordValidator.validateRecord(validAvroRecord)

    val inValidPlayCountValueCount = metricsReporter
      .asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
      .getInvalid(
        dynamicRecordValidator.className,
        playCountFieldParser.label,
        playCountFieldParser.companion.validatorIdentifier
      )

    val inValidUserIdCount = metricsReporter
      .asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
      .getInvalid(
        dynamicRecordValidator.className,
        userIdFieldParser.label,
        userIdFieldParser.companion.validatorIdentifier
      )

    (inValidPlayCountValueCount, inValidUserIdCount) should be ((1, 1))
  }

}
