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
package com.spotify.elitzur.dynamic.bq

import com.google.api.client.json.JsonObjectParser
import com.google.api.client.json.gson.GsonFactory
import com.google.api.services.bigquery.model.{TableRow, TableSchema}
import com.google.common.base.Charsets
import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.FieldAccessor
import com.spotify.elitzur.converters.avro.dynamic.schema.BqSchema
import com.spotify.elitzur.converters.avro.dynamic.validator.core.{
  DynamicAccessorCompanion,
  DynamicFieldParser
}
import com.spotify.elitzur.dynamic.helpers.DynamicAccessorValidatorTestUtils.TestMetricsReporter
import com.spotify.ratatool.scalacheck.tableRowOf
import org.scalacheck.Gen
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import collection.JavaConverters._

class BqFieldValidationArrayTest extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  import com.spotify.elitzur.dynamic.helpers._
  import Companions._

  private val tableSchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/BqRepeatedSchema.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  val tableRowGen: Gen[TableRow] = tableRowOf(tableSchema)

  implicit val metricsReporter: MetricsReporter =
    DynamicAccessorValidatorTestUtils.metricsReporter()

  override def afterEach(): Unit = {
    metricsReporter.asInstanceOf[TestMetricsReporter].cleanSlate()
  }

  val userInput: Array[DynamicFieldParser[BqSchema, TableRow]] = Array(
    new DynamicFieldParser(
      ".arrayLongs:NonNegativeLong",
      new DynamicAccessorCompanion[Long, NonNegativeLong],
      new FieldAccessor(BqSchema(tableSchema.getFields))
    )
  )

  it should "correctly count the valid fields" in {
    val testSetUp = new DynamicAccessorValidationHelpers(userInput)

    val validRecord = new TableRow()
      .set("arrayLongs", List(1L, 2L, -1L).asJava)

    // Validate the sample input
    testSetUp.dynamicRecordValidator.validateRecord(validRecord)

    val (arrayLongValidCount, arrayLongInvalidCount) = testSetUp.getValidAndInvalidCounts(
      ".arrayLongs:NonNegativeLong", NonNegativeLongCompanion)

    (arrayLongValidCount, arrayLongInvalidCount) should be((2, 1))
  }
}
