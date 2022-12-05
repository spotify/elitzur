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
package com.spotify.elitzur.dynamic.avro

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.FieldAccessor
import com.spotify.elitzur.converters.avro.dynamic.validator.core.{
  DynamicAccessorCompanion,
  DynamicFieldParser
}
import com.spotify.elitzur.dynamic.helpers.DynamicAccessorValidatorTestUtils.TestMetricsReporter
import com.spotify.elitzur.helpers.SampleAvroRecords
import com.spotify.elitzur.schemas.TestAvroTypes
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AvroFieldValidationBaseTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  import com.spotify.elitzur.dynamic.helpers._
  import Companions._

  implicit val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  override def afterEach(): Unit = {
    metricsReporter.asInstanceOf[TestMetricsReporter].cleanSlate()
  }

  val userInput: Array[DynamicFieldParser[Schema, GenericRecord]] = Array(
    new DynamicFieldParser(
      ".inner.playCount:NonNegativeLong",
      new DynamicAccessorCompanion[Long, NonNegativeLong],
      new FieldAccessor(TestAvroTypes.SCHEMA$)
    ),
    new DynamicFieldParser(
      ".inner.countryCode:CountryCode",
      new DynamicAccessorCompanion[String, CountryCode],
      new FieldAccessor(TestAvroTypes.SCHEMA$)
    )
  )

  it should "correctly count the valid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validAvroRecord = SampleAvroRecords.testAvroTypes(isValid = true)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount:NonNegativeLong", NonNegativeLongCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode:CountryCode", CountryCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((1, 0, 1, 0))
  }

  it should "correctly count the invalid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validAvroRecord = SampleAvroRecords.testAvroTypes(isValid = false)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validAvroRecord)

    val (playCountValidCount, playCountInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.playCount:NonNegativeLong", NonNegativeLongCompanion)

    val (countryCodValidCount, countryCodInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".inner.countryCode:CountryCode", CountryCompanion)

    (playCountValidCount, playCountInvalidCount,
      countryCodValidCount, countryCodInvalidCount) should be ((0, 1, 0, 1))
  }

}
