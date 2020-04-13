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
package com.spotify.elitzur

import com.spotify.elitzur._
import com.spotify.elitzur.converters.avro.AvroConverter
import com.spotify.elitzur.schemas.{TestAvroOut, TestAvroTypes}
import com.spotify.elitzur.validators._
import com.spotify.elitzur.converters.avro._
import com.spotify.ratatool.scalacheck._
import com.spotify.elitzur.Companions._
import com.spotify.elitzur.validators.{Unvalidated, Validator}
import org.scalacheck._

import scala.language.higherKinds


object ProfilingAvro {
  case class TestAvro(
                       userAge: AgeExample,
                       userLong: NonNegativeLongExample,
                       userFloat: Float,
                       inner: InnerNested
                     )

  case class InnerNested(countryCode: CountryCodeExample)

  implicit val metricsReporter: MetricsReporter = new MetricsReporter {
    val map : scala.collection.mutable.Map[String, Int] =
      scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    override def reportValid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.valid") += 1
    override def reportInvalid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.invalid") += 1
    override def toString: String = map.toString()
  }

  //scalastyle:off magic.number
  val avroRecords: Seq[TestAvroTypes] = Gen.listOfN(1000, avroOf[TestAvroTypes]).sample.get
  //scalastyle:on magic.number

  val c: AvroConverter[TestAvro] = implicitly[AvroConverter[TestAvro]]
  val v: Validator[TestAvro] = implicitly[Validator[TestAvro]]


  def main(args: Array[String]): Unit = {
    avroRecords
      .map(a => c.fromAvro(a, TestAvroTypes.SCHEMA$))
      .map(a => v.validateRecord(Unvalidated(a)))
      .map(a => c.toAvro(a.forceGet, TestAvroOut.SCHEMA$))
  }
}
