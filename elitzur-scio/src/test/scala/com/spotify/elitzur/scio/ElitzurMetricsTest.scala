package com.spotify.elitzur.scio

import com.spotify.elitzur.CountryCodeTesting
import com.spotify.elitzur.validators.ValidationStatus
import org.scalatest.{FlatSpec, Matchers, PrivateMethodTester}

//scalastyle:off no.whitespace.before.left.bracket
class ElitzurMetricsTest extends FlatSpec with PrivateMethodTester with Matchers {

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
