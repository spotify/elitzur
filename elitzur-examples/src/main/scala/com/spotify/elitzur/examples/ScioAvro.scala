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


import com.spotify.scio._
import com.spotify.elitzur.schemas.{InnerNestedType, TestAvroTypes}
import com.spotify.elitzur.scio._
import com.spotify.elitzur.validators._
import org.slf4j.LoggerFactory
import com.spotify.elitzur.examples.Companions._
import org.apache.beam.sdk.metrics.MetricName

// Example: Reading in Avro records within a Scio job and validating

// Usage:
// sbt "elitzur-examples/runMain com.spotify.elitzur.examples.ScioAvro --runner=DirectRunner"
// Dataflow requires additional params, see https://beam.apache.org/documentation/runners/dataflow/

object ScioAvro {

  private val logger = LoggerFactory.getLogger(this.getClass)

  case class User(
                   userAge: Age,
                   userLong: NonNegativeLong,
                   userFloat: Float,
                   inner: InnerNested
                 )

  case class InnerNested(countryCode: CountryCode)

  val builder :TestAvroTypes.Builder = TestAvroTypes.newBuilder()
  val innerBuilder : InnerNestedType.Builder = InnerNestedType.newBuilder()
  val avroRecords: Seq[TestAvroTypes] = Seq(
    // record with all fields valid
    builder
      .setUserAge(33L)
      .setUserLong(45L)
      .setUserFloat(4f)
      .setInner(innerBuilder.setCountryCode("US").setUserId("182").setPlayCount(72L).build())
      .build(),
    // record with invalid age
    builder
      .setUserAge(-33L)
      .setUserLong(45L)
      .setUserFloat(4f)
      .setInner(innerBuilder.setCountryCode("CA").setUserId("129").setPlayCount(43L).build())
      .build(),
    // record with invalid country code
    builder
      .setUserAge(33L)
      .setUserLong(45L)
      .setUserFloat(4f)
      .setInner(innerBuilder.setCountryCode("USA").setUserId("678").setPlayCount(201L).build())
      .build()
  )

  /*
  Common use case of Elitzur is to read in a schematized file in a standard format, e.g. Avro.

  We define case classes that specify validation types for fields we want to validate, in this
  example, Age, NonNegativeLong and CountryCode and validate records based on those validation types

  One can then go back to a standard file format, e.g. in this case back to Avro to persist the data
   */
  def main(cmdlineArgs: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(cmdlineArgs)
    // For reading in real avro files, can use
    // val records = sc.typedAvroFile[TestAvroTypes](inputPath) instead
    val records = sc.parallelize(avroRecords)

    records
      .fromAvro[User]
      .validate()

    // .toAvro[TestAvroOut]  can comment out these two lines so save avro output
    //.saveAsAvroFile(outputPath)

    val result = sc
      .run()
      .waitUntilDone()

    val elitzurCounters = ElitzurMetrics.getElitzurCounters(result)

    // log to display validation metrics
    logCounters(elitzurCounters)
  }

  def logCounters(counters: Map[MetricName, metrics.MetricValue[Long]]) : Unit = {
    val logString =
      counters
        .foldLeft("")((acc, d) => {
          s"$acc\n Counter ${d._1.toString} has value ${d._2.committed.getOrElse(0L).toString}"
        })

    logger.info(s"Logging Elitzur Counters: $logString \n Done logging Elitzur Counters")
  }


}
