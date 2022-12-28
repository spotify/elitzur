package com.spotify.elitzur.validators

import com.spotify.elitzur.validators.DynamicRecordValidatorTest.TestMetricsReporter
import com.spotify.elitzur.{AgeTesting, CountryCodeTesting, MetricsReporter}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

case class Outer(
    country: ValidationStatus[CountryCodeTesting],
    innerStatus: ValidationStatus[Inner],
    inner: Inner,
    age: AgeTesting,
    ageOpt: Option[AgeTesting],
    repeatedAge: List[AgeTesting],
    repeatedInner: List[Inner]
)

case class Inner(
    countryStatus: ValidationStatus[CountryCodeTesting],
    country: CountryCodeTesting,
    countryOpt: Option[CountryCodeTesting]
)

//scalastyle:off magic.number
class ValidatorTest extends AnyFlatSpec with Matchers {

  val inner = Inner(
    countryStatus = Unvalidated(CountryCodeTesting("US")),
    country = CountryCodeTesting("CA"),
    countryOpt = Some(CountryCodeTesting("SE"))
  )

  "Validator" should "validate valid record" in {
    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()
    val validator = Validator.gen[Outer]
    val result = validator.validateRecord(
      Unvalidated(
        Outer(
          country = Unvalidated(CountryCodeTesting("US")),
          innerStatus = Unvalidated(inner),
          inner = inner,
          age = AgeTesting(25L),
          ageOpt = Some(AgeTesting(45L)),
          repeatedAge = List(AgeTesting(50L), AgeTesting(10L)),
          repeatedInner = List(inner, inner, inner)
        )
      )
    )
    result.isValid shouldBe true
    val testMetrics = metricsReporter.asInstanceOf[TestMetricsReporter]
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "country",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "innerStatus.countryStatus",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "innerStatus.country",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "innerStatus.countryOpt",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "inner.countryStatus",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "inner.country",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "inner.countryOpt",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "age",
      "AgeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "ageOpt",
      "AgeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedAge",
      "AgeTesting"
    ) shouldEqual 2
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedInner.countryStatus",
      "CountryCodeTesting"
    ) shouldEqual 3
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedInner.country",
      "CountryCodeTesting"
    ) shouldEqual 3
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedInner.countryOpt",
      "CountryCodeTesting"
    ) shouldEqual 3
  }

  "Validator" should "validate invalid record" in {
    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()
    val validator = Validator.gen[Outer]
    val result = validator.validateRecord(
      Unvalidated(
        Outer(
          country = Unvalidated(CountryCodeTesting("FOO")),
          innerStatus = Unvalidated(inner),
          inner = inner,
          age = AgeTesting(25L),
          ageOpt = Some(AgeTesting(45L)),
          repeatedAge = List(AgeTesting(50L), AgeTesting(1000L)),
          repeatedInner = List(inner, inner, inner)
        )
      )
    )
    // PostValidationWrapper cannot be cast to class
    // com.spotify.elitzur.validators.Invalid in scala 2.12
    // result.asInstanceOf[Invalid[Outer]].fields shouldEqual Some(Set("country", "repeatedAge"))

    result.isValid shouldBe false
    val testMetrics = metricsReporter.asInstanceOf[TestMetricsReporter]
    testMetrics.getInvalid(
      "com.spotify.elitzur.validators.Outer",
      "country",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "innerStatus.countryStatus",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "innerStatus.country",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "innerStatus.countryOpt",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "inner.countryStatus",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "inner.country",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "inner.countryOpt",
      "CountryCodeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "age",
      "AgeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "ageOpt",
      "AgeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedAge",
      "AgeTesting"
    ) shouldEqual 1
    testMetrics.getInvalid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedAge",
      "AgeTesting"
    ) shouldEqual 1
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedInner.countryStatus",
      "CountryCodeTesting"
    ) shouldEqual 3
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedInner.country",
      "CountryCodeTesting"
    ) shouldEqual 3
    testMetrics.getValid(
      "com.spotify.elitzur.validators.Outer",
      "repeatedInner.countryOpt",
      "CountryCodeTesting"
    ) shouldEqual 3
  }
}
//scalastyle:on magic.number
