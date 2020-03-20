package com.spotify.elitzur

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object AvroClassConverterTest {
  case class TestTypes(userAge: Long,
                       userFloat: Float,
                       userLong: Long,
                       innerOpt: Option[Inner],
                       inner: Inner)

  case class Inner(userId: String, countryCode: String, playCount: Long)
}

class AvroConverterTest extends AnyFlatSpec with Matchers {

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
