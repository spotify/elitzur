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

package com.spotify.elitzur.helpers

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.dynamic.{
  DynamicValidationCompanion,
  DynamicValidationCompanionImplicits
}
import com.spotify.elitzur.types.Owner
import com.spotify.elitzur.validators._

import java.util.Locale


case object Blizzard extends Owner {
  override def name: String = "Blizzard"
}

case class NonNegativeLong(data: Long) extends BaseValidationType[Long] {
  override def checkValid: Boolean = data >= 0L
}

object NonNegativeLongCompanion extends BaseCompanion[Long, NonNegativeLong] {
  def validationType: String = "NonNegativeLong"

  def bigQueryType: String = "INTEGER"

  def apply(data: Long): NonNegativeLong = NonNegativeLong(data)

  def parse(data: Long): NonNegativeLong = NonNegativeLong(data)

  override def owner: Owner = Blizzard

  override def description: String = "Non negative long"
}

case class NonNegativeDouble(data: Double) extends BaseValidationType[Double] {
  override def checkValid: Boolean = data >= 0.0
}

object NonNegativeDoubleCompanion extends BaseCompanion[Double, NonNegativeDouble] {
  def validationType: String = "NonNegativeDouble"

  def bigQueryType: String = "FLOAT"

  def apply(data: Double): NonNegativeDouble = NonNegativeDouble(data)

  def parse(data: Double): NonNegativeDouble = NonNegativeDouble(data)

  override def owner: Owner = Blizzard

  override def description: String = "Non negative double"
}

case class CountryCode(data: String) extends BaseValidationType[String] {
  override def checkValid: Boolean = Locale.getISOCountries.contains(data)
}

object CountryCompanion extends BaseCompanion[String, CountryCode] {
  def validationType: String = "CountryCode"

  def bigQueryType: String = "STRING"

  def apply(data: String): CountryCode = CountryCode(data)

  def parse(data: String): CountryCode = CountryCode(data)

  def description: String = "Represents an ISO standard two-letter country code"

  def owner: Owner = Blizzard
}

object DynamicAccessorValidatorTestUtils {
  import DynamicValidationCompanionImplicits._

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

  val NonNegativeLongDynamicCompanion: DynamicValidationCompanion =
    new DynamicValidationCompanion(NonNegativeLongCompanion.validationType) {
      override val validator: Validator[_] = implicitly[Validator[NonNegativeLong]]
      override def dynamicParser: Any => Any = NonNegativeLongCompanion.dynamicParser
    }

  val NonNegativeDoubleDynamicCompanion: DynamicValidationCompanion =
    new DynamicValidationCompanion(NonNegativeDoubleCompanion.validationType) {
      override val validator: Validator[_] = implicitly[Validator[NonNegativeDouble]]
      override def dynamicParser: Any => Any = NonNegativeDoubleCompanion.dynamicParser
    }

  val CountryCodeDynamicCompanion: DynamicValidationCompanion =
    new DynamicValidationCompanion(CountryCompanion.validationType) {
      override val validator: Validator[_] = implicitly[Validator[CountryCode]]
      override def dynamicParser: Any => Any = CountryCompanion.dynamicParser
    }
}
