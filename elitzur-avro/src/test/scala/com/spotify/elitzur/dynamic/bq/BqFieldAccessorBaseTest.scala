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
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.SchemaToAccessorOpsExceptionMsg._
import com.spotify.elitzur.converters.avro.dynamic.schema.BqSchema
import com.spotify.ratatool.scalacheck.tableRowOf
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.{util => ju}

class BqFieldAccessorBaseTest extends AnyFlatSpec with Matchers {

  private val tableSimpleSchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/BqSimpleSchema.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  private val tableArraySchema = new JsonObjectParser(new GsonFactory)
    .parseAndClose(
      this.getClass.getResourceAsStream("/BqRepeatedSchema.json"),
      Charsets.UTF_8,
      classOf[TableSchema]
    )

  val tableRowGen: Gen[TableRow] = tableRowOf(tableSimpleSchema)

  it should "extract a primitive at the record root level" in {
    val testSimpleBqRecord = tableRowGen.sample.get
    val fn = new FieldAccessor(BqSchema(tableSimpleSchema.getFields))
      .getFieldAccessor(".userId")

    fn.accessorFns(testSimpleBqRecord) should be (testSimpleBqRecord.get("userId"))
  }

  it should "extract an array at the record root level" in {
    val testSimpleBqRecord = tableRowGen.sample.get
    val fn = new FieldAccessor(BqSchema(tableArraySchema.getFields))
      .getFieldAccessor(".arrayLongs")

    fn.accessorFns(testSimpleBqRecord) should be (testSimpleBqRecord.get("arrayLongs"))
  }

  it should "extract a nested record" in {
    val testSimpleBqRecord = tableRowGen.sample.get
    val fn = new FieldAccessor(BqSchema(tableSimpleSchema.getFields))
      .getFieldAccessor(".inner.userId")

    fn.accessorFns(testSimpleBqRecord) should be (
      testSimpleBqRecord.get("inner").asInstanceOf[ju.Map[String, Any]].get("userId"))
  }

  it should "extract a record if the field has _ in it" in {
    val testSimpleBqRecord = tableRowGen.sample.get
    val fn = new FieldAccessor(BqSchema(tableSimpleSchema.getFields))
      .getFieldAccessor("._user_id10")

    fn.accessorFns(testSimpleBqRecord) should be (testSimpleBqRecord.get("_user_id10"))
  }

  it should "throw an exception if the field is missing" in {
    val thrown = intercept[InvalidDynamicFieldException] {
      new FieldAccessor(BqSchema(tableSimpleSchema.getFields)).getFieldAccessor(".notRealField")
    }

    thrown.getMessage should include(".notRealField not found in")
  }

}
