package com.spotify.elitzur

import java.util.Locale

import com.spotify.elitzur.types.Owner
import Companions._
import com.spotify.elitzur.validators.{BaseCompanion, BaseValidationType, SimpleCompanionImplicit}

/**
  * This file contains 'fake' ValidationType & related implementations, used only to test that
  * validation runs consistently.
  *
  * Unfortunately we can't make this private,
  * because our Override Type Provider is called from Scio and not within elitzur
  */

/* OWNERS */

private[this] case object Blizzard extends Owner {
  override def name: String = "Blizzard"
}


/* VALIDATION TYPES */

object Companions {
  implicit val ageC: SimpleCompanionImplicit[Long, AgeExample] =
    SimpleCompanionImplicit(AgeExampleCompanion)
  implicit val ccC: SimpleCompanionImplicit[String, CountryCodeExample] =
    SimpleCompanionImplicit(CountryCodeExampleCompanion)
  implicit val nnlC: SimpleCompanionImplicit[Long, NonNegativeLongExample] =
    SimpleCompanionImplicit(NonNegativeLongExampleCompanion)
  implicit val brC: SimpleCompanionImplicit[String, BucketizedReferrerExample] =
    SimpleCompanionImplicit(BucketizedReferrerExampleCompanion)
}


case class CountryCodeExample(data: String)
  extends BaseValidationType[String] {
  override def checkValid: Boolean = Locale.getISOCountries.contains(data)
}

object CountryCodeExampleCompanion extends BaseCompanion[String, CountryCodeExample] {
  def validationType: String = "CountryCode"

  def bigQueryType: String = "STRING"

  def apply(data: String): CountryCodeExample = CountryCodeExample(data)

  def parse(data: String): CountryCodeExample = CountryCodeExample(data)

  def owner: Owner = Blizzard

  def description: String = "Represents an ISO standard two-letter country code"
}



case class AgeExample(data: Long)
  extends BaseValidationType[Long] {
  override def checkValid: Boolean = data > 0L && data < 150L
}

object AgeExampleCompanion extends BaseCompanion[Long, AgeExample] {
  def validationType: String = "Age"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): AgeExample = AgeExample(data)

  def parse(data: Long): AgeExample = AgeExample(data)

  def owner: Owner = Blizzard

  def description: String = "This represents an age above 0 and less than 150"
}


case class NonNegativeLongExample(data: Long)
  extends BaseValidationType[Long] {
  override def checkValid: Boolean = data >= 0L
}

object NonNegativeLongExampleCompanion extends BaseCompanion[Long, NonNegativeLongExample] {
  def validationType: String = "NonNegativeLong"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): NonNegativeLongExample = NonNegativeLongExample(data)

  def parse(data: Long): NonNegativeLongExample = NonNegativeLongExample(data)

  override def owner: Owner = Blizzard

  override def description: String = "Non negative long"
}

case class BucketizedReferrerExample(data: String)
  extends BaseValidationType[String] {
  private val values: Set[String] = Set(
    "home",
    "your_library",
    "search",
    "browse",
    "radio",
    "other")

  override def checkValid: Boolean = values.contains(data)
}

object BucketizedReferrerExampleCompanion
  extends BaseCompanion[String, BucketizedReferrerExample] {
  def validationType: String = "BucketizedReferrer"

  def bigQueryType: String = "STRING"

  def apply(data: String): BucketizedReferrerExample = BucketizedReferrerExample(data)

  def parse(data: String): BucketizedReferrerExample = BucketizedReferrerExample(data)

  def owner: Owner = Blizzard

  def description: String = "The page/tab in the mobile or desktop App from which stream was " +
    "initiated.  Possible values:  home, your_library, search, browse, radio, other"
}
