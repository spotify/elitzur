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

import java.util.Locale
import com.spotify.elitzur.scio._
import com.spotify.elitzur.validators.Invalid
import com.spotify.scio.{ContextAndArgs, ScioMetrics}
import com.spotify.scio.testing.PipelineSpec
import org.scalacheck.{Arbitrary, Gen}

import scala.collection.compat.immutable.ArraySeq

object TestClasses {
  case class Test(inner: Inner, countryCode: CountryCodeTesting)
  case class Inner(playCount: NonNegativeLongTesting)
  case class DynamicRecord(i: DynamicString, j: NonNegativeLongTesting)

  case class ListTest(t: List[CountryCodeTesting], a: List[String])
  case class VectorTest(t: Vector[CountryCodeTesting])
  case class SeqTest(t: Seq[CountryCodeTesting])
  case class ArrayTest(t: Array[CountryCodeTesting])
  // Test for nested Record too
  case class NestedRecordSequence(nested: ListTest)
}


object PipelineInput {
  import TestClasses._

  val validListInput =
    List(ListTest(List(CountryCodeTesting("US"), CountryCodeTesting("MX")), List("A", "B")))

  val validVectorInput =
    List(VectorTest(Vector(CountryCodeTesting("US"), CountryCodeTesting("MX"))))

  val invalidSeqInput = List(
    SeqTest(
      // Contains a valid country code but should still return invalid since
      // some of the values are invalid
      Seq(CountryCodeTesting("US"), CountryCodeTesting("sdfsdfd"), CountryCodeTesting("sdfsd"))
    )
  )

  val invalidArrayInput =
    List(ArrayTest(Array(CountryCodeTesting("12"), CountryCodeTesting("2121X"))))

  val nestedRecordValidInput =
    List(
      NestedRecordSequence(
        ListTest(validListInput.headOption.get.t, validListInput.headOption.get.a))
    )
}

object DummyPipeline {
  import PipelineInput._

  def main(args: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(args)
    sc.parallelize(validListInput).validate()
    sc.parallelize(invalidSeqInput).validate()
    sc.parallelize(invalidArrayInput).validate()
    sc.parallelize(validVectorInput).validate()
    sc.parallelize(nestedRecordValidInput).validate()

    sc.run().waitUntilDone()
  }
}

class ValidatorDoFnTest extends PipelineSpec {


  "Validator SCollection helper" should "validate valid records" in {
    val validRecord = TestClasses.Test(TestClasses.Inner(NonNegativeLongTesting(0)),
      CountryCodeTesting("US"))

    runWithData(Seq(validRecord))(sc => {
      sc.validate()
        .count
    }) shouldBe Seq(1)
  }

  "Validator SCollection helper" should "validateWithResult autogenerated valid records" in {
    val validRecordGen = for {
      nnl <- Gen.posNum[Long]
      cc <- Gen.oneOf(ArraySeq.unsafeWrapArray(Locale.getISOCountries))
    } yield TestClasses.Test(TestClasses.Inner(NonNegativeLongTesting(nnl)), CountryCodeTesting(cc))
    val numberToValidate = 100
    val validRecords = Gen.listOfN(numberToValidate, validRecordGen).sample.get

    runWithData(validRecords)(sc =>
      sc.validateWithResult().filter(_.isValid).count) shouldBe Seq(numberToValidate)
  }

  "Validator SCollection helper" should "validateWithResult autogenerated invalid records" in {
    val invalidRecordGen = for {
      nnl <- Gen.negNum[Long]
      cc <- Gen.numStr
    } yield TestClasses.Test(TestClasses.Inner(NonNegativeLongTesting(nnl)), CountryCodeTesting(cc))
    val numberToValidate = 100
    val invalidRecords = Gen.listOfN(numberToValidate, invalidRecordGen).sample.get

    runWithData(invalidRecords)(sc =>
      sc.validateWithResult().filter(_.isInvalid).count) shouldBe Seq(numberToValidate)
  }

  "Validator SCollection helper" should "validateWithResult autogenerated " +
    "invalid records and have invalid fields" in {
    val invalidRecordGen = for {
      nnl <- Gen.negNum[Long]
      cc <- Gen.numStr
    } yield TestClasses.Test(TestClasses.Inner(NonNegativeLongTesting(nnl)), CountryCodeTesting(cc))
    val numberToValidate = 100
    val invalidRecords = Gen.listOfN(numberToValidate, invalidRecordGen).sample.get

    val result = runWithData(invalidRecords)(sc =>
      sc.validateWithResult()
        .filter(_.isInvalid)
        .flatMap(_.asInstanceOf[Invalid[TestClasses.Test]].fields.getOrElse(Set.empty))
        .distinct
    )

    result.toSet shouldBe Set("inner", "inner.playCount", "countryCode")
  }

  "Validator SCollection" should "validate dynamic records" in {
    val dynamicGen: Gen[TestClasses.DynamicRecord] = for {
      s <- Arbitrary.arbString.arbitrary
      l <- Gen.posNum[Long]
    } yield TestClasses.DynamicRecord(DynamicString(s, Set(s)), NonNegativeLongTesting(l))

    val input = List(dynamicGen.sample.get)
    runWithData(input)(sc => {
      sc.validateWithResult().flatten
        .count
    }) shouldBe Seq(1)
  }

  "Validator SCollection" should "validate collection of Seq,Vector,List or Array " +
    "and set counters" in {
    JobTest[DummyPipeline.type ]
      .counters(_.size shouldBe 6)
      .counter(
        ScioMetrics.counter(
          "com.spotify.elitzur.TestClasses.SeqTest",
          "t/CountryCodeTesting/ElitzurInvalid"
        )
      )(_ shouldBe 2)
      .counter(
        ScioMetrics.counter(
          "com.spotify.elitzur.TestClasses.SeqTest",
          "t/CountryCodeTesting/ElitzurValid"
        )
      )(_ shouldBe 1)
      .counter(
        ScioMetrics.counter(
          "com.spotify.elitzur.TestClasses.ListTest",
          "t/CountryCodeTesting/ElitzurValid"
        )
      )(_ shouldBe 2)
      .counter(
        ScioMetrics.counter(
          "com.spotify.elitzur.TestClasses.ArrayTest",
          "t/CountryCodeTesting/ElitzurInvalid"
        )
      )(_ shouldBe 2)
      .counter(
        ScioMetrics.counter(
          "com.spotify.elitzur.TestClasses.VectorTest",
          "t/CountryCodeTesting/ElitzurValid"
        )
      )(_ shouldBe 2)
      .counter(
        ScioMetrics.counter(
          "com.spotify.elitzur.TestClasses.NestedRecordSequence",
          "nested.t/CountryCodeTesting/ElitzurValid"
        )
      )(_ shouldBe 2)
      .run()
  }
}
