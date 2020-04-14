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

import java.util.concurrent.TimeUnit

import com.spotify.elitzur.validators.{PostValidation, Unvalidated, Validator}
import com.spotify.elitzur.scio._
import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit}

import scala.language.higherKinds

object CaseClassesToValidate {
  case class ThreeFields(
                          age: AgeExample,
                          countryCode: CountryCodeExample,
                          nnl: NonNegativeLongExample)

  case class TenFieldsThreeValidation(
                                       age: AgeExample,
                                       countryCode: CountryCodeExample,
                                       nnl: NonNegativeLongExample,
                                       a: String,
                                       b: Long,
                                       c: Long,
                                       d: Double,
                                       e: String,
                                       f: String,
                                       g: String
                                     )


  case class TenFields(
                        age: AgeExample,
                        countryCode: CountryCodeExample,
                        nnl: NonNegativeLongExample,
                        br: BucketizedReferrerExample,
                        age2: AgeExample,
                        age3: AgeExample,
                        age4: AgeExample,
                        age5: AgeExample,
                        countryCode2: CountryCodeExample,
                        nnl2: NonNegativeLongExample
                      )


  case class TwentyFieldsTenValidation(
                                        age: AgeExample,
                                        countryCode: CountryCodeExample,
                                        nnl: NonNegativeLongExample,
                                        br: BucketizedReferrerExample,
                                        age2: AgeExample,
                                        age3: AgeExample,
                                        age4: AgeExample,
                                        age5: AgeExample,
                                        countryCode2: CountryCodeExample,
                                        nnl2: NonNegativeLongExample,
                                        a: String,
                                        b: Long,
                                        c: Long,
                                        d: Double,
                                        e: String,
                                        f: String,
                                        g: String,
                                        h: Long,
                                        i: Double,
                                        j: Long
                                      )


  case class FiveNestedFiveFields(field1: FiveFields,
                                  field2: FiveFields,
                                  field3: FiveFields,
                                  field4: FiveFields,
                                  field5: FiveFields)

  case class FiveFields(age: AgeExample,
                        countryCode: CountryCodeExample,
                        nnl: NonNegativeLongExample,
                        br: BucketizedReferrerExample,
                        age2: AgeExample
                       )
}

object CaseClassValidators {

  import CaseClassesToValidate._

  def genThreeFields(): Validator[ThreeFields] = {
    Validator.gen[ThreeFields]
  }

  def genTenFieldThreeV(): Validator[TenFieldsThreeValidation] = {
    Validator.gen[TenFieldsThreeValidation]
  }

  def genTenFields(): Validator[TenFields] = {
    Validator.gen[TenFields]
  }

  def genTwentyFieldTenV(): Validator[TwentyFieldsTenValidation] = {
    Validator.gen[TwentyFieldsTenValidation]
  }

  def genFiveNestedFive(): Validator[FiveNestedFiveFields] = {
    Validator.gen[FiveNestedFiveFields]
  }

  val threeVal: Validator[ThreeFields] = genThreeFields()
  val tenVal: Validator[TenFields] = genTenFields()
  val tenV3Val: Validator[TenFieldsThreeValidation] = genTenFieldThreeV()
  val twentyV10Val: Validator[TwentyFieldsTenValidation] = genTwentyFieldTenV()
  val fiveNFiveVal: Validator[FiveNestedFiveFields] = genFiveNestedFive()
}

object Fields {
  import CaseClassesToValidate._

  val Three = ThreeFields(
    AgeExample(10L),
    CountryCodeExample("US"),
    NonNegativeLongExample(0L)
  )

  val Ten = TenFields(
    AgeExample(1L),
    CountryCodeExample("US"),
    NonNegativeLongExample(0L),
    BucketizedReferrerExample("home"),
    AgeExample(5L),
    AgeExample(5L),
    AgeExample(5L),
    AgeExample(5L),
    CountryCodeExample("SE"),
    NonNegativeLongExample(5L)
  )

  val Twenty = TwentyFieldsTenValidation(
    AgeExample(1L),
    CountryCodeExample("US"),
    NonNegativeLongExample(0L),
    BucketizedReferrerExample("home"),
    AgeExample(5L),
    AgeExample(5L),
    AgeExample(5L),
    AgeExample(5L),
    CountryCodeExample("SE"),
    NonNegativeLongExample(5L),
    "",
    1L,
    0L,
    1.0,
    "",
    "",
    "",
    1L,
    1.0,
    1L
  )

  val TenV3 = TenFieldsThreeValidation(
    AgeExample(1L),
    CountryCodeExample("US"),
    NonNegativeLongExample(0L),
    "",
    0L,
    0L,
    1.0,
    "",
    "",
    ""
  )

  val fiveN5 = FiveNestedFiveFields(
    FiveFields(
      AgeExample(1L),
      CountryCodeExample("US"),
      NonNegativeLongExample(2L),
      BucketizedReferrerExample("home"),
      AgeExample(10L)
    ),
    FiveFields(
      AgeExample(2L),
      CountryCodeExample("SE"),
      NonNegativeLongExample(5L),
      BucketizedReferrerExample("search"),
      AgeExample(30L)
    ),
    FiveFields(
      AgeExample(3L),
      CountryCodeExample("CA"),
      NonNegativeLongExample(9L),
      BucketizedReferrerExample("browse"),
      AgeExample(11L)
    ),
    FiveFields(
      AgeExample(4L),
      CountryCodeExample("GY"),
      NonNegativeLongExample(32L),
      BucketizedReferrerExample("radio"),
      AgeExample(15L)
    ),
    FiveFields(
      AgeExample(5L),
      CountryCodeExample("MX"),
      NonNegativeLongExample(50L),
      BucketizedReferrerExample("other"),
      AgeExample(60L)
    )
  )
}

class Benchmarking {
  import CaseClassValidators._
  import CaseClassesToValidate._
  import Fields._

  @Benchmark @BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
  def validateThree(): PostValidation[ThreeFields] = {
    threeVal.validateRecord(Unvalidated(Three))
  }

  @Benchmark @BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
  def validateTen(): PostValidation[TenFields] = {
    tenVal.validateRecord(Unvalidated(Ten))
  }

  @Benchmark @BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
  def validateTwenty(): PostValidation[TwentyFieldsTenValidation] = {
    twentyV10Val.validateRecord(Unvalidated(Twenty))
  }

  @Benchmark @BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
  def validateTenV3(): PostValidation[TenFieldsThreeValidation] = {
    tenV3Val.validateRecord(Unvalidated(TenV3))
  }

  @Benchmark @BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.SECONDS)
  def validatefiveN5(): PostValidation[FiveNestedFiveFields] = {
    fiveNFiveVal.validateRecord(Unvalidated(fiveN5))
  }
}
