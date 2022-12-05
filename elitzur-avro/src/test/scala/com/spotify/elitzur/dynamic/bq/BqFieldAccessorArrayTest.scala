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
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.FieldAccessor
import com.spotify.elitzur.converters.avro.dynamic.schema.BqSchema
import com.spotify.ratatool.scalacheck.tableRowOf
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.{util => ju}
import scala.collection.JavaConverters._

class BqFieldAccessorArrayTest extends AnyFlatSpec with Matchers {

  private val tableSchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/BqRepeatedSchema.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  val tableRowGen: Gen[TableRow] = tableRowOf(tableSchema)
  val testArrayBqRecord = tableRowGen.sample.get

  it should "extract generic records in an array" in {
    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: [{"userId": "one"}, {"userId": "two"}]
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".innerArrayRoot[]")

    fn.accessorFns(testArrayBqRecord) should be (testArrayBqRecord.get("innerArrayRoot"))
  }

  it should "extract a field from generic records in an array" in {
    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: ["one", "two"]
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".innerArrayRoot[].userId")

    fn.accessorFns(testArrayBqRecord) should be (
      testArrayBqRecord
        .get("innerArrayRoot").asInstanceOf[ju.List[ju.Map[String, Any]]].asScala
        .map(_.get("userId")).asJava
    )
  }

  it should "extract a field from nested generic records in an array" in {
    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -1}}"},
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -5}}"}
    //    ]}
    // Output: [-1, -5]
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".innerArrayRoot[].deepNestedRecord.recordId")

    fn.accessorFns(testArrayBqRecord) should be (
      testArrayBqRecord
        .get("innerArrayRoot").asInstanceOf[ju.List[ju.Map[String, Any]]].asScala
        .map(_.get("deepNestedRecord")).asJava.asInstanceOf[ju.List[ju.Map[String, Any]]].asScala
        .map(_.get("recordId")).asJava
    )
  }

  it should "flatten the resulting array" in {
    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [1, 2, 3, 4]
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".innerArrayRoot[].innerArrayInsideRecord[]")

    fn.accessorFns(testArrayBqRecord) should be (
      testArrayBqRecord
        .get("innerArrayRoot").asInstanceOf[ju.List[ju.Map[String, Any]]].asScala
        .flatMap(_.get("innerArrayInsideRecord").asInstanceOf[ju.List[Long]].asScala).asJava
    )
  }

  it should "not flatten the resulting array" in {
    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [[1, 2], [3, 4]]
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".innerArrayRoot[].innerArrayInsideRecord")

    fn.accessorFns(testArrayBqRecord) should be(
      testArrayBqRecord
        .get("innerArrayRoot").asInstanceOf[ju.List[ju.Map[String, Any]]].asScala
        .map(_.get("innerArrayInsideRecord").asInstanceOf[ju.List[Long]]).asJava
    )
  }

  it should "flatten resulting array with a nullable field in the path" in {
    // Input: {"innerArrayRoot": [
    //    {"deeperArrayNestedRecord": {"DeeperArray": [1, 2]}},
    //    {"deeperArrayNestedRecord": {"DeeperArray": [3, 4]}}
    //    ]}
    // Output: [1, 2, 3, 4]
    val fn = new FieldAccessor(BqSchema(tableSchema.getFields))
      .getFieldAccessor(".innerArrayRoot[].deeperArrayNestedRecord.DeeperArray[]")

    val expected = {
      val res = new ju.ArrayList[Any]
      testArrayBqRecord
        .get("innerArrayRoot").asInstanceOf[ju.List[ju.Map[String, Any]]].asScala
        .map(_.get("deeperArrayNestedRecord").asInstanceOf[ju.Map[String, Any]]).asJava
        .forEach(x => if (x == null) res.add(null) else {
          x.get("DeeperArray").asInstanceOf[ju.List[Int]].forEach(z => res.add(z))
        })
      res
    }

    fn.accessorFns(testArrayBqRecord) should be (expected)
  }

}
