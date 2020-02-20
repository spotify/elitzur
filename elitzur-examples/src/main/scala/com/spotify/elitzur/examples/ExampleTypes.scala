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
package com.spotify.elitzur.examples

import java.util.Locale

import com.spotify.elitzur.types.Owner
import com.spotify.elitzur.validators.{BaseCompanion, BaseValidationType, SimpleCompanionImplicit}

case object Blizzard extends Owner {
  override def name: String = "Blizzard"
}

object Companions {
  implicit val ageC: SimpleCompanionImplicit[Long, Age] =
    SimpleCompanionImplicit(AgeCompanion)
  implicit val ccC: SimpleCompanionImplicit[String, CountryCode] =
    SimpleCompanionImplicit(CountryCompanion)
  implicit val nnlC: SimpleCompanionImplicit[Long, NonNegativeLong] =
    SimpleCompanionImplicit(NonNegativeLongCompanion)
}

case class CountryCode(data: String) extends BaseValidationType[String] {
  override def checkValid: Boolean = Locale.getISOCountries.contains(data)
}

case class Age(data: Long) extends BaseValidationType[Long] {
  override def checkValid: Boolean = data > 0L && data < 150L
}

case class NonNegativeLong(data: Long) extends BaseValidationType[Long] {
  override def checkValid: Boolean = data >= 0L
}

object CountryCompanion extends BaseCompanion[String, CountryCode] {
  def validationType: String = "CountryCode"

  def bigQueryType: String = "STRING"

  def apply(data: String): CountryCode = CountryCode(data)

  def parse(data: String): CountryCode = CountryCode(data)

  def description: String = "Represents an ISO standard two-letter country code"

  def owner: Owner = Blizzard
}

object AgeCompanion extends BaseCompanion[Long, Age] {
  def validationType: String = "Age"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): Age = Age(data)

  def parse(data: Long): Age = Age(data)

  def owner: Owner = Blizzard

  def description: String = "This represents an age above 0 and less than 150"
}


object NonNegativeLongCompanion extends BaseCompanion[Long, NonNegativeLong] {
  def validationType: String = "NonNegativeLong"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): NonNegativeLong = NonNegativeLong(data)

  def parse(data: Long): NonNegativeLong = NonNegativeLong(data)

  override def owner: Owner = Blizzard

  override def description: String = "Non negative long"
}

