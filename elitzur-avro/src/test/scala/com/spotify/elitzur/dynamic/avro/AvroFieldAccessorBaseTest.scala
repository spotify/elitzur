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
package com.spotify.elitzur.dynamic.avro

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.SchemaToAccessorOpsExceptionMsg._
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.FieldAccessor
import com.spotify.elitzur.helpers.SampleAvroRecords.{
  innerNestedSample,
  testAvroArrayTypes,
  testAvroTypes
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.apache.avro.SchemaBuilder
import org.apache.avro.generic.GenericRecordBuilder

class AvroFieldAccessorBaseTest extends AnyFlatSpec with Matchers {

  it should "extract a primitive at the record root level" in {
    val testSimpleAvroRecord = innerNestedSample()
    val fn = new FieldAccessor(testSimpleAvroRecord.getSchema)
      .getFieldAccessor(".userId")

    fn.accessorFns(testSimpleAvroRecord) should be (testSimpleAvroRecord.getUserId)
  }

  it should "extract an array at the record root level" in {
    val testSimpleAvroRecord = testAvroArrayTypes
    val fn = new FieldAccessor(testSimpleAvroRecord.getSchema)
      .getFieldAccessor(".arrayLongs")

    fn.accessorFns(testSimpleAvroRecord) should be (testSimpleAvroRecord.getArrayLongs)
  }

  it should "extract a nested record" in {
    val testSimpleAvroRecord = testAvroTypes()
    val fn = new FieldAccessor(testSimpleAvroRecord.getSchema)
      .getFieldAccessor(".inner.userId")

    fn.accessorFns(testSimpleAvroRecord) should be (testSimpleAvroRecord.getInner.getUserId)
  }

  it should "extract a record if the field has _ in it" in {
    val schema = SchemaBuilder
      .builder.record("record").fields.requiredLong("_user_id10").endRecord
    val testSimpleAvroRecord = new GenericRecordBuilder(schema).set("_user_id10", 1L).build
    val fn = new FieldAccessor(testSimpleAvroRecord.getSchema)
      .getFieldAccessor("._user_id10")

    fn.accessorFns(testSimpleAvroRecord) should be (testSimpleAvroRecord.get("_user_id10"))
  }

  it should "throw an exception if the field is missing" in {
    val testSimpleAvroRecord = testAvroTypes()
    val thrown = intercept[InvalidDynamicFieldException] {
      new FieldAccessor(testSimpleAvroRecord.getSchema)
        .getFieldAccessor(".notRealField")
    }

    thrown.getMessage should include(".notRealField not found in")
  }
}
