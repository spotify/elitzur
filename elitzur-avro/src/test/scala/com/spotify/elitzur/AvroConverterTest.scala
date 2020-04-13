package com.spotify.elitzur

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.ByteBuffer
import org.apache.avro.generic.GenericData
import org.apache.avro.specific.SpecificData
import org.apache.avro.message.{BinaryMessageEncoder, BinaryMessageDecoder}
import com.spotify.elitzur.converters.avro.AvroConverter
import enumeratum.EnumEntry.Snakecase
import enumeratum._
import org.apache.avro.Schema

object AvroClassConverterTest {
  case class TestTypes(userAge: Long,
                       userFloat: Float,
                       userLong: Long,
                       innerOpt: Option[Inner],
                       inner: Inner)

  case class Inner(userId: String, countryCode: String, playCount: Long)

  sealed trait EnumValue extends EnumEntry with Snakecase
  object EnumValue extends Enum[EnumValue] {
    val values = findValues
    case object SnakeCaseAaa extends EnumValue
    case object SnakeCaseBbb extends EnumValue
    case object SnakeCaseCcc extends EnumValue
  }
  case class TestEnum(testEnum: EnumValue, optTestEnum: Option[EnumValue])
}

class AvroConverterTest extends AnyFlatSpec with Matchers {

  it should "round-trip via a generic record" in {
    import AvroClassConverterTest._
    import com.spotify.elitzur.converters.avro._
    import com.spotify.elitzur.schemas.TestAvroEnum

    val schema: Schema = TestAvroEnum.getClassSchema
    val converter: AvroConverter[TestEnum] = implicitly
    val decoder = new BinaryMessageDecoder[TestAvroEnum](new SpecificData(), schema)

    val a: TestEnum = TestEnum(EnumValue.SnakeCaseBbb, Some(EnumValue.SnakeCaseCcc))
    val rec: GenericData.Record = converter.toAvro(a, schema).asInstanceOf[GenericData.Record]
    val encoder = new BinaryMessageEncoder[GenericData.Record](new GenericData(), schema)
    val bytes: ByteBuffer =  encoder.encode(rec)
    val converted: TestAvroEnum = decoder.decode(bytes)

    val b: TestEnum = converter.fromAvro(converted, schema)
    assert(a == b)
  }

  it should "work on nested optional records w/toAvro" in {
    import AvroClassConverterTest._
    import com.spotify.elitzur.converters.avro._
    import com.spotify.elitzur.schemas._

    val a: TestTypes = TestTypes(0L, 0F, 0L, Some(Inner("", "", 0L)), Inner("", "", 0L))
    val converter: AvroConverter[TestTypes] = implicitly
    converter.toAvro(a, TestAvroTypes.getClassSchema)
  }

  it should "work on nested optional records w/toAvroDefault" in {
    import AvroClassConverterTest._
    import com.spotify.elitzur.converters.avro._
    import com.spotify.elitzur.schemas._

    val a: TestTypes = TestTypes(0L, 0F, 0L, Some(Inner("", "", 0L)), Inner("", "", 0L))
    val converter: AvroConverter[TestTypes] = implicitly

    val inner = InnerNestedType.newBuilder()
      .setUserId("")
      .setCountryCode("")
      .setPlayCount(0L)
      .build()

    val testAvroTypeR = TestAvroTypes.newBuilder()
      .setUserAge(0L)
      .setUserFloat(0F)
      .setUserLong(0L)
      .setInnerOpt(inner)
      .setInner(inner)
      .build()

    converter.toAvroDefault(a, testAvroTypeR)
  }
}
