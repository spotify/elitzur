/*
 * Copyright 2020 Spotify AB.
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
package com.spotify.elitzur.scio

import com.spotify.elitzur.CountryCodeTesting
import com.spotify.elitzur.validators.ValidationStatus
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.PrivateMethodTester

//scalastyle:off no.whitespace.before.left.bracket
class ElitzurMetricsTest extends AnyFlatSpec with PrivateMethodTester with Matchers {

  case class Test(test: Int)

  case class HasValidationTypes(inner: CountryCodeTesting,
                                innerOption: Option[CountryCodeTesting])
  case class HasNestedValidationTypes(outer: HasValidationTypes,
                                      outerOption: Option[HasValidationTypes])

  case class HasWrappedValidationTypes(inner: ValidationStatus[CountryCodeTesting],
                                       innerOption: ValidationStatus[Option[CountryCodeTesting]])
  case class HasNestedWrappedValidationTypes(outer: ValidationStatus[HasWrappedValidationTypes],
                                             outerOption:
                                              ValidationStatus[Option[HasValidationTypes]])

  case class HasMixedWrapping(outer: ValidationStatus[HasWrappedValidationTypes],
                              outerOption: ValidationStatus[Option[HasValidationTypes]])

  "getValidationTypeFromCaseClass" should "return unqualified validation type name" in {
    val getValidationTypeFromCaseClass = PrivateMethod[String]('getValidationTypeFromCaseClass)
    val countryCodeName = ElitzurMetrics invokePrivate
      getValidationTypeFromCaseClass(classOf[HasValidationTypes], "inner")
    countryCodeName shouldBe "CountryCodeTesting"
  }

  private def testGetValidationTypeFromCaseClass(className: Class[_]) = {
    val getValidationTypeFromCaseClass = PrivateMethod[String]('getValidationTypeFromCaseClass)
    val countryCodeName = ElitzurMetrics invokePrivate
      getValidationTypeFromCaseClass(className, "outer.inner")
    countryCodeName shouldBe "CountryCodeTesting"
    val countryCodeOptionName = ElitzurMetrics invokePrivate
      getValidationTypeFromCaseClass(className, "outer.innerOption")
    countryCodeOptionName shouldBe "CountryCodeTesting"
    val optionCountryCodeName = ElitzurMetrics invokePrivate
      getValidationTypeFromCaseClass(className, "outerOption.inner")
    optionCountryCodeName shouldBe "CountryCodeTesting"
    val optionCountryCodeOptionName = ElitzurMetrics invokePrivate
      getValidationTypeFromCaseClass(className, "outerOption.innerOption")
    optionCountryCodeOptionName shouldBe "CountryCodeTesting"
  }

  "getValidationTypeFromCaseClass" should "work for nested fields" in {
    testGetValidationTypeFromCaseClass(classOf[HasNestedValidationTypes])
  }

  "getValidationTypeFromCaseClass" should "work for wrapped fields and records" in {
    testGetValidationTypeFromCaseClass(classOf[HasNestedWrappedValidationTypes])
  }

  "getValidationTypeFromCaseClass" should
    "work for records where some inner fields are wrapped and some aren't" in {
    testGetValidationTypeFromCaseClass(classOf[HasMixedWrapping])
  }
}
//scalastyle:on no.whitespace.before.left.bracket
