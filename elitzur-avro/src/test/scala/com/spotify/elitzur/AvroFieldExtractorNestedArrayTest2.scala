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

package com.spotify.elitzur

import com.spotify.elitzur.converters.avro.qaas.AvroFieldExtractor
import com.spotify.elitzur.schemas.{InnerComplexType, TestComplexArrayTypes, TestComplexSchemaTypes}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.matching.Regex
import java.{util => ju}
import scala.collection.convert.Wrappers

class AvroFieldExtractorNestedArrayTest2 extends AnyFlatSpec with Matchers {

  import helpers.SampleAvroRecords._

  def combineFns(fns: List[AvroOperation]): Any => Any =
    fns.map(_.ops.fn).reduceLeftOption((f, g) => f andThen g).get

  /**
   * Simple tests
   */
  it should "extract a primitive at the record root level" in {
    val testSimpleAvroRecord = innerNestedSample()
    val fn = combineFns(
        AvroFieldExtractorV2.getAvroValue("userId", testSimpleAvroRecord.getSchema)
    )
    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getUserId)
  }

  it should "extract an array at the record root level" in {
    val testSimpleAvroRecord = testAvroRecord(2)
    val fn = combineFns(
      AvroFieldExtractorV2.getAvroValue("arrayLongs", testSimpleAvroRecord.getSchema)
    )
    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getArrayLongs)
  }

  it should "extract a nested record" in {
    val testSimpleAvroRecord = testAvroRecord(2)
    val avroPath = "innerOpt.userId"

    val fns = AvroFieldExtractorV2.getAvroValue(avroPath, testSimpleAvroRecord.getSchema)
    val fn = combineFns(fns)

    fns.map(_.ops) should be (List(GenericRecordOperation(3), GenericRecordOperation(0)))
    fn(testSimpleAvroRecord) should be (testSimpleAvroRecord.getInnerOpt.getUserId)
  }

  /**
   * union/null tests
   */
  it should "extract a null from an Union schema type v2" in {
    // Input: {"optRecord": null}
    // Output: null
    val testNullRecord = TestComplexSchemaTypes.newBuilder().setOptRecord(null).build
    val avroPath = "optRecord.optString"

    val fns = AvroFieldExtractorV2.getAvroValue(avroPath, testNullRecord.getSchema)
    val fn = combineFns(fns)

    val expectedFns: List[OperationBase] = List[OperationBase](
      GenericRecordOperation(0), UnionNullOperation(GenericRecordOperation(0)))

    fns.map(_.ops) should be (expectedFns)
    fn(testNullRecord) should be (testNullRecord.getOptRecord)
  }

  it should "extract a null from a nested Union Avro schema type v2" in {
    // Input: {"optRecord": {"optString": null}}
    // Output: null
    val testInnerNullRecord = TestComplexSchemaTypes.newBuilder()
      .setOptRecord(InnerComplexType.newBuilder().setOptString(null).build).build
    val avroPath = "optRecord.optString"

    val fns = AvroFieldExtractorV2.getAvroValue(avroPath, testInnerNullRecord.getSchema)
    val fn = combineFns(fns)

    val expectedFns: List[OperationBase] = List[OperationBase](
      GenericRecordOperation(0), UnionNullOperation(GenericRecordOperation(0)))

    fns.map(_.ops) should be (expectedFns)
    fn(testInnerNullRecord) should be (testInnerNullRecord.getOptRecord.getOptString)
  }

  it should "extract a primitive from a Union Avro schema type v2" in {
    // Input: {"optRecord": {"optString": "abc"}}
    // Output: "abc"
    val testInnerNonNullRecord = TestComplexSchemaTypes.newBuilder()
      .setOptRecord(InnerComplexType.newBuilder().setOptString("abc").build).build
    val avroPath = "optRecord.optString"

    val fns = AvroFieldExtractorV2.getAvroValue(avroPath, testInnerNonNullRecord.getSchema)
    val fn = combineFns(fns)

    val expectedFns: List[OperationBase] = List[OperationBase](
      GenericRecordOperation(0), UnionNullOperation(GenericRecordOperation(0)))

    fns.map(_.ops) should be (expectedFns)
    fn(testInnerNonNullRecord) should be (testInnerNonNullRecord.getOptRecord.getOptString)
  }

  /**
   * array tests
   */
  import collection.JavaConverters._
  val testArrayRecord: TestComplexArrayTypes = testComplexArrayTypes

  it should "extract generic records in an array" in {

    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: [{"userId": "one"}, {"userId": "two"}]
    val fns = AvroFieldExtractorV2.getAvroValue("innerArrayRoot", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (testArrayRecord.getInnerArrayRoot)
  }

  it should "extract a field from generic records in an array" in {

    // Input: {"innerArrayRoot": [{"userId": "one"}, {"userId": "two"}]}
    // Output: ["one", "two"]
    val fns = AvroFieldExtractorV2.getAvroValue("innerArrayRoot.userId",
      testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getUserId).asJava)
  }

  it should "extract a field from nested generic records in an array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -1}}"},
    //    {"innerArrayInsideRecord": "deepNestedRecord": {"recordId": -5}}"}
    //    ]}
    // Output: [-1, -5]
    val fns = AvroFieldExtractorV2.getAvroValue(
      "innerArrayRoot.deepNestedRecord.recordId", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getDeepNestedRecord.getRecordId).asJava)
  }

  it should "extract an array within an array" in {

    // Input: {"innerArrayRoot": [
    //    {"innerArrayInsideRecord": [1, 2]},
    //    {"innerArrayInsideRecord": [3, 4]}
    //    ]}
    // Output: [[1, 2], [3, 4]]

    val fns = AvroFieldExtractorV2.getAvroValue(
      "innerArrayRoot.innerArrayInsideRecord", testArrayRecord.getSchema)
    val fn = combineFns(fns)

    fn(testArrayRecord) should be (
      testArrayRecord.getInnerArrayRoot.asScala.map(_.getInnerArrayInsideRecord).asJava)
  }

}

object AvroFieldExtractorV2 {
  private val hasNextLeaf: Regex = """^([a-zA-Z0-9]*)\.(.*)""".r
  private val isLeafNode: Regex = """^([a-zA-Z0-9]*)$""".r

  def getAvroValue(
    path: String,
    avroSchema: Schema,
    baseFunList: List[AvroOperation] = List.empty[AvroOperation]
  ): List[AvroOperation] = {
    path match {
      case hasNextLeaf(field, rest) =>
        val funThingy = new BaseThingy(path, field, Some(rest))
        val avroOp = funThingy.schemaFun(avroSchema)
        if (avroOp.rest.isDefined) {
          getAvroValue(avroOp.rest.get, avroOp.schema, baseFunList :+ avroOp)
        } else {
          baseFunList :+ avroOp
        }
      case isLeafNode(field) =>
        val funThingy = new BaseThingy(path, field)
        val avroOp = funThingy.schemaFun(avroSchema)
        baseFunList :+ avroOp
    }
  }
}

class BaseThingy(path: String, field: String, rest: Option[String] = None) {

  private val PRIMITIVES: Set[Schema.Type] =
    Set(Schema.Type.STRING, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.BOOLEAN,
      Schema.Type.BYTES, Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.NULL)

  // scalastyle:off cyclomatic.complexity
  def schemaFun(schema: Schema): AvroOperation = {
    schema.getType match {
      case Schema.Type.RECORD =>
        val childSchema = schema.getField(field)
        AvroOperation(GenericRecordOperation(childSchema.pos), childSchema.schema, rest)
      case Schema.Type.UNION =>
        // assumes Union type is used specifically for nullability - remove the null schema
        schema.getTypes.removeIf(_.getType == Schema.Type.NULL)
        val innerAvroOp = schemaFun(schema.getTypes.get(0))
        AvroOperation(UnionNullOperation(innerAvroOp.ops), innerAvroOp.schema, rest)
      case Schema.Type.ARRAY =>
        val arraySchema = schema.getElementType
        val innerOps = AvroFieldExtractorV2.getAvroValue(path, arraySchema)
        getAvroArrayOperation(arraySchema, field, innerOps)
      case schema if PRIMITIVES.contains(schema) => throw new Exception("abc")
      case Schema.Type.MAP | Schema.Type.FIXED | Schema.Type.ENUM => throw new Exception("abc")
    }
  }
  // scalastyle:on cyclomatic.complexity

  // unfortunate function to get around Scala's weird array casting
  def getAvroArrayOperation(
    arraySchema: Schema, innerField: String, innerOps: List[AvroOperation]): AvroOperation = {
    val ops = innerOps.map(_.ops)
    val remainingField = innerOps.lastOption.flatMap(_.rest)
    arraySchema.getField(innerField).schema.getType match {
      case Schema.Type.RECORD | Schema.Type.ARRAY => AvroOperation(
        ArrayOperation(ops, CastArrayListOperation()), arraySchema, remainingField)
      case schema if PRIMITIVES.contains(schema) => AvroOperation(
        ArrayOperation(ops, CastSeqWrapperOperation()), arraySchema, remainingField)
    }
  }
}

case class AvroOperation(ops: OperationBase, schema: Schema, rest: Option[String])

trait OperationBase {
  def fn: Any => Any
}

case class GenericRecordOperation(idx: Int) extends OperationBase {
  override def fn: Any => Any = (o: Any) => o.asInstanceOf[GenericRecord].get(idx)
}

case class UnionNullOperation(op: OperationBase) extends OperationBase {
  override def fn: Any => Any = (o: Any) => if (o == null) o else op.fn(o)
}

case class ArrayOperation[T <: ju.List[_]](ops: List[OperationBase], c: CastOperationBase[T])
  extends OperationBase {
  override def fn: Any => Any = (o: Any) => {
    val res = new ju.ArrayList[Any]
    c.cast(o).forEach(elem => res.add(composedFn.get(elem)))
    res
  }

  val composedFn: Option[Any => Any] = ops.map(_.fn).reduceLeftOption((f, g) => f andThen g)
}

trait CastOperationBase[T <: ju.List[_]] {
  def cast: Any => T = (o: Any) => o.asInstanceOf[T]
}

case class CastArrayListOperation() extends CastOperationBase[ju.ArrayList[_]]

case class CastSeqWrapperOperation() extends CastOperationBase[Wrappers.SeqWrapper[_]]