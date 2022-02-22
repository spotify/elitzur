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
  QaasAvroRecordValidator,
  QaasValidationCompanion,
  QaasValidationCompanionImplicits
}
import com.spotify.elitzur.helpers._
import com.spotify.elitzur.validators.Validator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object DynamicRecordValidatorTest {
  class TestMetricsReporter extends MetricsReporter {
    val map : scala.collection.mutable.Map[String, Int] =
      scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    override def reportValid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.valid") += 1
    override def reportInvalid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.invalid") += 1
    override def toString: String = map.toString()
    def getValid(className: String, fieldName: String, validationType: String): Int =
      map(s"$className.$fieldName.$validationType.valid")
    def getInvalid(className: String, fieldName: String, validationType: String): Int =
      map(s"$className.$fieldName.$validationType.invalid")
  }
  def metricsReporter(): MetricsReporter = new TestMetricsReporter
}

object QaasValidationCompanionProviderTest {
  import QaasValidationCompanionImplicits._

  // Expected to be made by jinja
  def getValidationCompanion: Map[String, QaasValidationCompanion] = {
    Map[String, QaasValidationCompanion](
      NonNegativeLongCompanion.validationType.toUpperCase ->
        new QaasValidationCompanion(NonNegativeLongCompanion.validationType) {
          override val validator: Validator[_] = implicitly[Validator[NonNegativeLong]]
          override def validatorCheckParser: Any => Any = NonNegativeLongCompanion.parseAvroObj
        },
      NonNegativeDoubleCompanion.validationType.toUpperCase ->
        new QaasValidationCompanion(NonNegativeDoubleCompanion.validationType) {
          override val validator: Validator[_] = implicitly[Validator[NonNegativeDouble]]
          override def validatorCheckParser: Any => Any = NonNegativeDoubleCompanion.parseAvroObj
        },
      CountryCompanion.validationType.toUpperCase ->
        new QaasValidationCompanion(CountryCompanion.validationType) {
          override val validator: Validator[_] = implicitly[Validator[CountryCode]]
          override def validatorCheckParser: Any => Any = CountryCompanion.parseAvroObj
        }
      )
  }
}

class DynamicRecordValidatorTest extends AnyFlatSpec with Matchers {
  import helpers.SampleAvroRecords._
  import QaasValidationCompanionProviderTest._

  it should "process beginning to end" in {

    val avroFieldWithValidation: Array[String] = Array(
      ".inner.playCount:NonNegativeLong",
      ".inner.userId:CountryCode"
    )

    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()

    val qaasValidationCompanionMap: Map[String, QaasValidationCompanion] = getValidationCompanion

    val tester = new QaasAvroRecordValidator(avroFieldWithValidation, qaasValidationCompanionMap)

    tester.validateRecord(testAvroTypes)

    val thisMetricType = tester.validationInputs.headOption.get

    metricsReporter.asInstanceOf[DynamicRecordValidatorTest.TestMetricsReporter].getValid(
      tester.className,
      thisMetricType.label,
      thisMetricType.qaasValidationCompanion.validatorIdentifier
    ) shouldEqual 1
  }

}
