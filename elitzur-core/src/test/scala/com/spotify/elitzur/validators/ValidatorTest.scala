package com.spotify.elitzur.validators

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

class ValidatorTest extends AnyFlatSpec with Matchers {

  val inner = Inner(
    countryStatus = Unvalidated(CountryCodeTesting("US")),
    country = CountryCodeTesting("CA"),
    countryOpt = Some(CountryCodeTesting("SE"))
  )

  implicit val testMetricsReporter: MetricsReporter = new MetricsReporter {
    override def reportValid(
        className: String,
        fieldName: String,
        validationTypeName: String
    ): Unit = ()

    override def reportInvalid(
        className: String,
        fieldName: String,
        validationTypeName: String
    ): Unit = ()
  }

  "Validator" should "validate record" in {
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
    result.isInvalid shouldBe false
  }
}
