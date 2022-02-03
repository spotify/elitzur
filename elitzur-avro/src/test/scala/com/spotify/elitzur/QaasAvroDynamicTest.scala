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

import com.spotify.elitzur.converters.avro.qaas.{
  AvroFieldExtractor,
  QaasAvroRecordValidator,
  QaasValidationCompanion,
  QaasValidationCompanionImplicits
}
import com.spotify.elitzur.helpers._
import com.spotify.elitzur.validators.Validator
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util

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
  def getQaasValidationCompanion: Map[String, QaasValidationCompanion] = {
    Map[String, QaasValidationCompanion](
      NonNegativeLongCompanion.validationType.toUpperCase -> QaasValidationCompanion(
        implicitly[Validator[NonNegativeLong]],
        NonNegativeLongCompanion.parseAvroObj,
        NonNegativeLongCompanion.validationType
      ),
      NonNegativeDoubleCompanion.validationType.toUpperCase -> QaasValidationCompanion(
        implicitly[Validator[NonNegativeDouble]],
        NonNegativeDoubleCompanion.parseAvroObj,
        NonNegativeDoubleCompanion.validationType
      ),
      CountryCompanion.validationType.toUpperCase -> QaasValidationCompanion(
        implicitly[Validator[CountryCode]],
        CountryCompanion.parseAvroObj,
        CountryCompanion.validationType
      )
    )
  }
}

class DynamicRecordValidatorTest extends AnyFlatSpec with Matchers {
  import com.spotify.elitzur.schemas._

  val inner = InnerNestedType.newBuilder()
    .setUserId("")
    .setCountryCode("")
    .setPlayCount(0L)
    .build()

  val innerV2 = InnerNestedType.newBuilder()
    .setUserId("abc")
    .setCountryCode("xyz")
    .setPlayCount(5L)
    .build()

  val javaInnerList: util.List[InnerNestedType] = new util.ArrayList[InnerNestedType]
  javaInnerList.add(inner)
  javaInnerList.add(innerV2)

  val innerNestedArrayType = InnerNestedArrayType.newBuilder()
    .setArrayInnerNested(javaInnerList)
    .build()

  val innerNestedWithArrayFieldType = InnerNestedWithArrayFieldType.newBuilder()
    .setUserId("abc")
    .setInnerNested(innerNestedArrayType)
    .build()

  val arrayInnerNested = new util.ArrayList[InnerNestedWithArrayFieldType]
  arrayInnerNested.add(innerNestedWithArrayFieldType)
  arrayInnerNested.add(innerNestedWithArrayFieldType)

  val javaLongList: util.List[java.lang.Long] = new util.ArrayList[java.lang.Long]
  javaLongList.add(9)
  javaLongList.add(7)

  val testAvroTypeR = TestAvroArrayTypes.newBuilder()
    .setUserAge(0L)
    .setUserFloat(0F)
    .setUserLong(0L)
    .setInnerOpt(inner)
    .setArrayLongs(javaLongList)
    .setArrayInnerNested(arrayInnerNested)
    .build()

  it should "pass new parser" in {
//    val res1 = AvroFieldExtractor.getAvroValue("innerOpt.userId", testAvroTypeR)
//    res1
    val res2 = AvroFieldExtractor.getAvroValue(
      "arrayInnerNested.innerNested.arrayInnerNested.countryCode", testAvroTypeR)
    res2
//    val res3 = AvroFieldExtractor.getAvroValue("innerOptNull.userId", testAvroTypeR) // not handled
  }

  it should "process beginning to end" in {
    val avroFieldWithValidation: Array[String] = Array(
      "innerOpt.playCount:NonNegativeLong",
      "innerOpt.userId:CountryCode"
    )

    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()
    val qaasValidationCompanionMap: Map[String, QaasValidationCompanion] =
      QaasValidationCompanionProviderTest.getQaasValidationCompanion

    val tester = new QaasAvroRecordValidator(avroFieldWithValidation, qaasValidationCompanionMap)

    tester.validateRecord(testAvroTypeR)

    val thisMetricType = tester.validationInputs.headOption.get

    metricsReporter.asInstanceOf[DynamicRecordValidatorTest.TestMetricsReporter].getValid(
      tester.className,
      thisMetricType.label,
      thisMetricType.qaasValidationCompanion.validatorIdentifier
    ) shouldEqual 1
  }

}
