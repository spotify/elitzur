package com.spotify.elitzur

import java.util.Locale

import com.spotify.elitzur.validators.{BaseCompanion, BaseValidationType, SimpleCompanionImplicit}

//scalastyle:off line.size.limit
import com.spotify.elitzur.types.Owner
import com.spotify.elitzur.validators.{DynamicCompanion, DynamicCompanionImplicit, DynamicValidationType}
//scalastyle:on line.size.limit

private[elitzur] object ValidationTypeImplicits {
  implicit val ageC: SimpleCompanionImplicit[Long, AgeTesting] =
    SimpleCompanionImplicit(AgeTestingCompanion)

  implicit val ccC: SimpleCompanionImplicit[String, CountryCodeTesting] =
    SimpleCompanionImplicit(CountryCodeTestingCompanion)

  implicit val nnlC: SimpleCompanionImplicit[Long, NonNegativeLongTesting] =
    SimpleCompanionImplicit(NonNegativeLongTestingCompanion)

  implicit val nndC: SimpleCompanionImplicit[Double, NonNegativeDoubleTesting] =
    SimpleCompanionImplicit(NonNegativeDoubleTestingCompanion)

  implicit val dtC
  : DynamicCompanionImplicit[String, Set[String], DynamicString] =
    DynamicCompanionImplicit(DynamicString)
}

private[this] case object Blizzard extends Owner {
  override def name: String = "Blizzard"
}

import com.spotify.elitzur.ValidationTypeImplicits._

//This file contains 'fake' ValidationType & related implementations, used only to test that
//validation runs consistently.
//
//Unfortunately we can't make these types private,
//because our Override Type Provider is called from Scio and not within elitzur
case class CountryCodeTesting(data: String) extends BaseValidationType[String] {
  override def checkValid: Boolean = Locale.getISOCountries.contains(data)
}

object CountryCodeTestingCompanion extends BaseCompanion[String, CountryCodeTesting] {
  def validationType: String = "CountryCode"

  def bigQueryType: String = "STRING"

  def apply(data: String): CountryCodeTesting = CountryCodeTesting(data)

  def parse(data: String): CountryCodeTesting = CountryCodeTesting(data)

  def owner: Owner = Blizzard

  def description: String = "Represents an ISO standard two-letter country code"
}

case class AgeTesting(data: Long) extends BaseValidationType[Long] {
  override def checkValid: Boolean = data > 0L && data < 150L
}

object AgeTestingCompanion extends BaseCompanion[Long, AgeTesting] {
  def validationType: String = "Age"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): AgeTesting = new AgeTesting(data)

  def parse(data: Long): AgeTesting = AgeTesting(data)

  def owner: Owner = Blizzard

  def description: String = "This represents an age above 0 and less than 150"
}

case class NonNegativeLongTesting(data: Long) extends BaseValidationType[Long] {
  override def checkValid: Boolean = data >= 0L
}

object NonNegativeLongTestingCompanion extends BaseCompanion[Long, NonNegativeLongTesting] {
  def validationType: String = "NonNegativeLong"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): NonNegativeLongTesting = NonNegativeLongTesting(data)

  def parse(data: Long): NonNegativeLongTesting = NonNegativeLongTesting(data)

  override def owner: Owner = Blizzard

  override def description: String = "Non negative long"
}

case class NonNegativeDoubleTesting(data: Double) extends BaseValidationType[Double] {
  override def checkValid: Boolean = data >= 0.0
}

object NonNegativeDoubleTestingCompanion extends BaseCompanion[Double, NonNegativeDoubleTesting] {
  def validationType: String = "NonNegativeDouble"

  def bigQueryType: String = "FLOAT"

  def apply(data: Double): NonNegativeDoubleTesting = NonNegativeDoubleTesting(data)

  def parse(data: Double): NonNegativeDoubleTesting = NonNegativeDoubleTesting(data)

  override def owner: Owner = Blizzard

  override def description: String = "Non negative double"
}

//scalastyle:off line.size.limit
case class DynamicString(data: String, arg: Option[Set[String]] = None)
  extends DynamicValidationType[String, Set[String], DynamicString] {
  override def checkValid: Boolean = {
    arg.exists(_.contains(data))
  }

}
//scalastyle:on line.size.limit

object DynamicString  extends DynamicCompanion[String, Set[String], DynamicString] {
  def validationType: String = "DynamicCountryCode"

  def bigQueryType: String = "STRING"

  override def apply(data: String): DynamicString = DynamicString(data, None)

  override def setArg(i: DynamicString, a: Set[String]): DynamicString =
    i.setArg(a)

  override def parseWithArg(data: String, arg: Set[String]): DynamicString =
    DynamicString(data, Some(arg))

  override def parse(data: String): DynamicString =
    DynamicString(data, None)

  def owner: Owner = Blizzard

  def description: String = "Represents a dynamically defined testing type"
}
