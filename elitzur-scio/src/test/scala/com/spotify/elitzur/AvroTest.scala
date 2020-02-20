package com.spotify.elitzur


import com.spotify.elitzur.converters.avro._
import com.spotify.elitzur.example._
import ValidationTypeImplicits._
import com.spotify.elitzur.scio._
import com.spotify.elitzur.validators._
import com.spotify.scio.testing.PipelineSpec
import com.spotify.skeleton.schema._
import com.spotify.scio.values.SCollection
import enumeratum.{Enum, EnumEntry}
import org.apache.avro.util.Utf8
import org.apache.beam.sdk.Pipeline.PipelineExecutionException
import org.scalacheck.{Arbitrary, Gen}
import com.spotify.ratatool.scalacheck._

import scala.collection.JavaConverters._


sealed trait TestEnum extends EnumEntry with Product with Serializable

object TestEnum extends Enum[TestEnum] {
  val values = findValues

  case object EndSong extends TestEnum
  case object EndVideo extends TestEnum
  case object StreamProgress extends TestEnum
}

object AvroTestCaseClasses {
  case class UnnestedValidated(countryCode: ValidationStatus[CountryCodeTesting],
                               userId: String,
                               playCount: Long)
  case class Inner(countryCode: CountryCodeTesting,
                   userId: String,
                   playCount: NonNegativeLongTesting)
  case class InnerPartial()
  case class NestedPartial(inner: InnerPartial)
  case class Nested(inner: Inner)
  case class NestedValidated(inner: UnnestedValidated)
  case class NestedAll(userAge: AgeTesting,
                       userLong: NonNegativeLongTesting,
                       userOptionalLong: Option[NonNegativeLongTesting],
                       innerOpt: Option[Inner],
                       inner: Inner,
                       // user would have to define these themselves, as they're custom objects
                       // TODO: weird magnolia error around Java enums
                       // userTypeEnum: UserType,
                       // see within my DoFns for definition of FIXED_ELEMENT Converters
                       // userFixed: FIXED_ELEMENT,
                       // TODO: repeated types not currently supported
                       // userArrayString:util.List[String],
                       // userMapStringLong:util.Map[String, java.lang.Long],
                       latitude: Array[Byte],
                       longitude: Array[Byte],
                       userDouble: Double,
                       userBoolean: Boolean,
                       userFloat: Float,
                       enumTest: TestEnum)
  case class EncryptedCC(latitude: NonNegativeDoubleTesting)
  case class UnnestedNoValidation(countryCode: String,
                                  userId: String,
                                  playCount: Long)
  case class SimpleNullableRecord(userOptionalLong: NonNegativeLongTesting)
  case class DynamicRecord(stringField: DynamicString)
  case class RepeatedInnerRecordCC(stringField: CountryCodeTesting,
                                   longField: NonNegativeLongTesting)
  case class RepeatedRecordCC(repeatedRecord: Seq[RepeatedInnerRecordCC],
                              repeatedField: List[CountryCodeTesting])
  case class PartialRepeated(repeatedField: List[CountryCodeTesting])
  case class WithEnumField(enumTest: TestEnum)

  case class NullableNested(inner: Option[Inner])
}

object AvroArbs {
  import AvroTestCaseClasses._
  import org.scalacheck.ScalacheckShapeless._

  implicit def validationStatusArb[T](implicit a: Arbitrary[T]): Arbitrary[ValidationStatus[T]] = {
    Arbitrary(a.arbitrary.flatMap{t =>
      Gen.oneOf[ValidationStatus[T]](Unvalidated(t), Valid(t), Invalid(t))
    })
  }

  object ArbInstances {
    val arbUnnested: Arbitrary[Inner] = implicitly[Arbitrary[Inner]]
    val arbUnnestedValidated: Arbitrary[UnnestedValidated] =
      implicitly[Arbitrary[UnnestedValidated]]
    val arbNested: Arbitrary[Nested] = implicitly[Arbitrary[Nested]]
    val arbNestedValidated: Arbitrary[NestedValidated] = implicitly[Arbitrary[NestedValidated]]
    val arbNestedAll: Arbitrary[NestedAll] = implicitly[Arbitrary[NestedAll]]
  }
}

//scalastyle:off magic.number
class AvroTest extends PipelineSpec {
  import AvroArbs.ArbInstances._
  import AvroTestCaseClasses._
  val scLength = 5

  "AvroCaseClassConverter" should "convert and then validate for a simple record" in {
    val testRecordsValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    runWithData(testRecordsValid)(sc =>
      sc
        .fromAvro[Inner]
        .validate()
        .count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter" should "round trip Avro for a simple record" in {
    //TODO: Use the actual generators from validation testing types
    val testRecords = Gen.listOfN(scLength,
      Gen.const("US").flatMap(s => Gen.posNum[Long].map(l => (l, s))).flatMap{case (l, s) =>
        arbUnnested.arbitrary.map(_.copy(countryCode = CountryCodeTesting(s),
          playCount = NonNegativeLongTesting(l)))
      }).sample.get

    runWithContext { sc =>
      val r: SCollection[Inner] = sc.parallelize(testRecords)
        .toAvro[InnerNestedType]
        .fromAvro[Inner]

      r should containInAnyOrder(testRecords)
    }
  }

  "AvroCaseClassConverter" should "round trip Avro for a repeated record" in {
    val testRecords = Gen.listOfN(scLength,
      for {
        cc <- Gen.oneOf("US", "SE", "CA", "UK", "GY").map(CountryCodeTesting(_))
        nnl <- Gen.chooseNum(0, 150).map(NonNegativeLongTesting(_))
        cc2 <- Gen.oneOf("US", "SE", "CA", "UK", "GY").map(CountryCodeTesting(_))
        nnl2 <- Gen.chooseNum(0, 150).map(NonNegativeLongTesting(_))
      } yield RepeatedRecordCC(
        Seq(RepeatedInnerRecordCC(cc, nnl), RepeatedInnerRecordCC(cc2, nnl2)),
        List(cc, cc2))
    ).sample.get

    runWithContext { sc =>
      val r: SCollection[RepeatedRecordCC] = sc.parallelize(testRecords)
        .toAvro[RepeatedRecord]
        .fromAvro[RepeatedRecordCC]

      r should containInAnyOrder(testRecords)
    }
  }

  "AvroCaseClassConverter" should "convert and validate for a simple record wrapped in " +
    "validator" in {
    val testRecordsValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    runWithData(testRecordsValid)(sc =>
      sc.map(e => implicitly[AvroConverter[UnnestedValidated]].fromAvro(e, e.getSchema))
        .validate().count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter" should "round trip Avro for a simple record with wrapped status" in {
    //TODO: Use the actual generators from validation testing types
    val testRecords = Gen.listOfN(scLength,
      Gen.const("US").flatMap(s => Gen.posNum[Long].map(l => (l, s))).flatMap{case (l, s) =>
        arbUnnestedValidated.arbitrary.map(_.copy(
          countryCode = Unvalidated(CountryCodeTesting(s)),
          playCount = l))
      }).sample.get

    runWithContext { sc =>
      val r: SCollection[UnnestedValidated] = sc.parallelize(testRecords)
        .toAvro[InnerNestedType]
        .fromAvro[UnnestedValidated]

      r should containInAnyOrder(testRecords)
    }
  }

  "AvroCaseClassConverter" should "convert and then validate for a " +
    "non-wrapped-in-validator nested record" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val testRecordsValid: Seq[TestAvroTypes] = testRecordsInnerValid.map(tr =>
      avroOf[TestAvroTypes].amend(Gen.const(tr))(_.setInner).sample.get)

    runWithData(testRecordsValid)(sc =>
      sc
        .fromAvro[Nested]
        .validate()
        .map(_.inner.countryCode)
        .count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter" should "round trip for a simple nested record" in {
    val testRecordsInner =
      Gen.const("US").flatMap(s => Gen.posNum[Long].map(l => (l, s))).flatMap{case (l, s) =>
        arbUnnested.arbitrary.map(_.copy(countryCode = CountryCodeTesting(s),
          playCount = NonNegativeLongTesting(l)))
      }

    val testRecordsValid = Gen.listOfN(scLength, testRecordsInner.flatMap{i =>
      arbNested.arbitrary.map(n => n.copy(inner = i))}).sample.get

    runWithContext { sc =>
      val r = sc.parallelize(testRecordsValid)
        .toAvro[TestNestedRecord]
        .fromAvro[Nested]
        .validate()
      r should containInAnyOrder(testRecordsValid)
    }
  }

  "AvroCaseClassConverter" should "convert and then validate for a " +
    "wrapped-in-validator nested record" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val testRecordsValid: Seq[TestAvroTypes] = testRecordsInnerValid.map(tr =>
      avroOf[TestAvroTypes].amend(Gen.const(tr))(_.setInner).sample.get)

    runWithData(testRecordsValid)(sc =>
      sc
        .fromAvro[NestedAll]
        .validate()
        .map(_.inner.countryCode) // should be able to get the value
        .count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter" should "round trip for a nested record with a wrapped field" in {
    val testRecordsInner =
      Gen.const("US").flatMap(s => Gen.posNum[Long].map(l => (l, s))).flatMap{case (l, s) =>
        arbUnnestedValidated.arbitrary.map(_.copy(countryCode = Unvalidated(CountryCodeTesting(s)),
          playCount = l))
      }

    val testRecordsValid = Gen.listOfN(scLength, testRecordsInner.flatMap{i =>
      arbNestedValidated.arbitrary.map(n => n.copy(inner = i))}).sample.get

    runWithContext { sc =>
      val r = sc.parallelize(testRecordsValid)
        .toAvro[TestNestedRecord]
        .fromAvro[NestedValidated]
      r should containInAnyOrder(testRecordsValid)
    }
  }

  "AvroCaseClassConverter" should "convert and then validate for a " +
    "record with ALL the avro types" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val testRecordsValid: Seq[TestAvroTypes] = testRecordsInnerValid.map(tr =>
      avroOf[TestAvroTypes]
        .amend(Gen.const(tr))(_.setInner)
        .amend(Gen.oneOf(tr, null))(_.setInnerOpt)
        .amend(Gen.const(50L))(_.setUserAge)
        .amend(Gen.posNum[Long])(_.setUserLong)
        .amend(Gen.posNum[Long])(_.setUserOptionalLong)
        .sample.get)

    runWithData(testRecordsValid)(sc =>
      sc
        .fromAvro[NestedAll]
        .validate()
        .map(_.userOptionalLong.get) // check that Option was created successfully
        // since we didn't allow it to be null we should be able to call .get and get the number
        .count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter default" should "convert and then convert w/default for a " +
    "record with ALL the avro types, from a case class with some defaults missing" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val innerOnlyValid: Seq[TestNestedRecord] = testRecordsInnerValid.map(tr =>
      avroOf[TestNestedRecord].amend(Gen.const(tr))(_.setInner).sample.get)

    val testRecordValid = {
      val gr = specificRecordOf[TestAvroTypes].sample.get
      gr.put("inner", testRecordsInnerValid.headOption.get)
      gr.put("innerOpt", testRecordsInnerValid.headOption.get)
      gr.put("userAge", 50L)
      gr.put("userLong", 100L)
      gr.put("userOptionalLong", 100L)
      gr
    }

    runWithData(innerOnlyValid)(sc =>
      sc.fromAvro[Nested]
        .toAvroDefault[TestAvroTypes](testRecordValid)
        .count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter default" should "convert and then convert w/default for a " +
    "record with ALL the avro types, from a case class with some nested fields with defaults " +
    "missing" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val innerOnlyValid: Seq[TestNestedRecord] = testRecordsInnerValid.map(tr =>
      avroOf[TestNestedRecord].amend(Gen.const(tr))(_.setInner).sample.get)

    val testRecordValid = {
      val gr = specificRecordOf[TestAvroTypes].sample.get
      gr.put("inner", testRecordsInnerValid.headOption.get)
      gr.put("innerOpt", testRecordsInnerValid.headOption.get)
      gr.put("userAge", 50L)
      gr.put("userLong", 100L)
      gr.put("userOptionalLong", 100L)
      gr
    }

    runWithData(innerOnlyValid)(sc =>
      sc.fromAvro[NestedPartial]
        .toAvroDefault[TestAvroTypes](testRecordValid)
        .count
    ) shouldBe Seq(scLength)
  }

  "AvroCaseClassConverter default" should "convert and then convert w/default for a case class" +
    "with a repeated record field" in {
    val testRecordDefault = Gen.listOfN(scLength, avroOf[RepeatedRecord]).sample.get.headOption.get
    val testRecords = Gen.listOfN(scLength, avroOf[RepeatedRecord]).sample.get

    runWithData(testRecords)(sc =>
      sc.fromAvro[PartialRepeated]
        .toAvroDefault[RepeatedRecord](testRecordDefault)
      .map(_.getRepeatedRecord.get(0).getLongField)
    ) shouldEqual
      testRecords.map(e => testRecordDefault.getRepeatedRecord.get(0).getLongField).toSeq
  }

  "AvroCaseClassConverter" should "fail to convert for a " +
    "record with a null Avro type" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val testRecordsValid: Seq[TestAvroTypes] = testRecordsInnerValid.map(tr =>
      avroOf[TestAvroTypes]
        .amend(Gen.const(tr))(_.setInner)
        .amend(Gen.oneOf(tr, null))(_.setInnerOpt)
        .amend(Gen.const(50L))(_.setUserAge)
        .amend(Gen.posNum[Long])(_.setUserLong)
        .amend(Gen.const(null))(_.setUserOptionalLong)
        .sample.get)

    val exc = the[PipelineExecutionException] thrownBy {
      runWithData(testRecordsValid)(sc =>
        sc
          .fromAvro[SimpleNullableRecord]
          .validate()
          .count
      ) }

    val npe = exc.getCause

    npe.getClass.getCanonicalName shouldBe classOf[NullPointerException].getCanonicalName
    npe.getMessage shouldBe "Expected non-optional field to be non-null in Avro. " +
      "To fix, declare your Elitzur case class with any nullable Avro union fields " +
      "wrapped in options instead of declaring as the type directly."
  }


  "AvroCaseClassConverter" should "convert but not validate for a " +
    "record with optional avro types" in {
    val testRecordsInnerValid = Gen.listOfN(scLength, avroOf[InnerNestedType]
      .amend(Gen.const("US"))(_.setCountryCode)
      .amend(Gen.posNum[Long])(_.setPlayCount))
      .sample.get

    val testRecordsValid: Seq[TestAvroTypes] = testRecordsInnerValid.map(tr =>
      avroOf[TestAvroTypes]
        .amend(Gen.const(tr))(_.setInner)
        .amend(Gen.oneOf(tr, null))(_.setInnerOpt)
        .amend(Gen.const(50L))(_.setUserAge)
        .amend(Gen.posNum[Long])(_.setUserLong)
        .amend(Gen.posNum[Long])(_.setUserOptionalLong)
        .sample.get)

    runWithData(testRecordsValid)(sc =>
      sc
        .fromAvro[NestedAll]
        .map(_.userOptionalLong.get) // check that Option was created successfully
        // since we didn't allow it to be null we should be able to call .get and get the number
        .count
    ) shouldBe Seq(scLength)
  }

  "String Conversion" should "work with org.apache.avro.util.Utf8" in {
    val avroWithUtf8 = InnerNestedType.newBuilder()
      .setUserId(new Utf8("test"))
      .setPlayCount(400L)
      .setCountryCode(new Utf8("US"))
      .build()
    val cc = implicitly[AvroConverter[UnnestedNoValidation]]
      .fromAvro(avroWithUtf8, avroWithUtf8.getSchema)
    cc.userId shouldEqual "test"
    cc.countryCode shouldEqual "US"
  }

  "toAvroDefault" should "work with nullable nested records" in {
    val testRecordDefault = NullableNestedRecord.newBuilder().setInner(null).build()
    val testRecord = avroOf[NullableNestedRecord].amend(Gen.const(null))(_.setInner).sample.get
    val c = implicitly[AvroConverter[NullableNested]]

    c.toAvroDefault(c.fromAvro(testRecord, NullableNestedRecord.SCHEMA$), testRecordDefault)
  }

  "AvroCaseClassConverter" should "convert and then validate for a dynamic record with arg" in {
    val testRecord = DynamicType.newBuilder().setStringField("String").build()

    runWithData(Seq(testRecord))(sc =>
      sc
        .fromAvro[DynamicRecord]
        .map(c => c.copy(c.stringField.setArg(Set("String"))))
        .validateWithResult()
    ) shouldEqual Seq(Valid(DynamicRecord(DynamicString("String", Set("String")))))
  }

  "AvroCaseClassConverter" should "round trip for record with an enum field" in {
    val testRecords = Gen.listOfN(10, avroOf[EnumType])
      .sample
      .get

    runWithContext { sc =>
      val r = sc.parallelize(testRecords)
        .fromAvro[WithEnumField]
        .toAvro[EnumType]
      r should containInAnyOrder(testRecords)
    }
  }
}
//scalastyle:on magic.number
