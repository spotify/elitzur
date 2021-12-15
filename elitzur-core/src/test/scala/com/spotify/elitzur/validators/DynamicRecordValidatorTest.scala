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

package com.spotify.elitzur.validators

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.util.Locale

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.types.Owner
import com.spotify.elitzur.validators.DynamicRecordValidatorTest.TestMetricsReporter

case object Blizzard extends Owner {
  override def name: String = "Blizzard"
}

object Companions {
  implicit val nnlC: SimpleCompanionImplicit[Long, NonNegativeLong] =
    SimpleCompanionImplicit(NonNegativeLongCompanion)
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


class DynamicRecordValidatorTest extends AnyFlatSpec with Matchers {

  it should "validate a simple case" in {
    val label = "label"
    val className = "com.spotify.DynamicClass"
    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()
    val nnlValidator = implicitly[Validator[NonNegativeLong]]
    val recordValidator = DynamicRecordValidator(
      Array(nnlValidator).asInstanceOf[Array[Validator[Any]]], Array(label))
    recordValidator.validateRecord(
      Unvalidated(Seq(NonNegativeLong(1L)).asInstanceOf[Seq[Any]]),
      outermostClassName = Some(className)
    )
    metricsReporter.asInstanceOf[TestMetricsReporter].getValid(
      className,
      label,
      NonNegativeLongCompanion.validationType
    ) shouldEqual 1
  }

//  it should "validate a repeated value" in {
//    val label = "label"
//    val className = "com.spotify.DynamicClass"
//    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()
//    val repeatedValidator = implicitly[Validator[Seq[NonNegativeLong]]]
//    val recordValidator = DynamicRecordValidator(
//      Array(repeatedValidator).asInstanceOf[Array[Validator[Any]]], Array(label))
//    recordValidator.validateRecord(
//      Unvalidated(Seq(Seq(NonNegativeLong(1L), NonNegativeLong(-1L))).asInstanceOf[Seq[Any]]),
//      outermostClassName = Some(className)
//    )
//    print(metricsReporter.toString)
//    metricsReporter.asInstanceOf[TestMetricsReporter].getValid(
//      className,
//      label,
//      NonNegativeLongCompanion.validationType
//    ) shouldEqual 1
//    metricsReporter.asInstanceOf[TestMetricsReporter].getInvalid(
//      className,
//      label,
//      NonNegativeLongCompanion.validationType
//    ) shouldEqual 1
//  }

  it should "validate multiple fields" in {
    val label1 = "label1"
    val label2 = "label2"
    val className = "com.spotify.DynamicClass"
    implicit val metricsReporter: MetricsReporter = DynamicRecordValidatorTest.metricsReporter()
    val nnlValidator = implicitly[Validator[NonNegativeLong]]
    val recordValidator = DynamicRecordValidator(
      Array(nnlValidator, nnlValidator).asInstanceOf[Array[Validator[Any]]], Array(label1, label2))
    recordValidator.validateRecord(
      Unvalidated(Seq(NonNegativeLong(1L), NonNegativeLong(-1L)).asInstanceOf[Seq[Any]]),
      outermostClassName = Some(className)
    )
    metricsReporter.asInstanceOf[TestMetricsReporter].getValid(
      className,
      label1,
      NonNegativeLongCompanion.validationType
    ) shouldEqual 1
    metricsReporter.asInstanceOf[TestMetricsReporter].getInvalid(
      className,
      label2,
      NonNegativeLongCompanion.validationType
    ) shouldEqual 1
  }
}
