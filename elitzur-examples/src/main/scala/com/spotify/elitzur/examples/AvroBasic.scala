package com.spotify.elitzur.examples

import com.spotify.elitzur._
import com.spotify.elitzur.validators._
import com.spotify.elitzur.examples.Companions._
import com.spotify.elitzur.schemas.{InnerNestedType, TestAvroOut, TestAvroTypes}
import com.spotify.elitzur.validators._
import com.spotify.elitzur.converters.avro._

// Example:  Common use case of Elitzur is to have a schematized dataset, e.g. Avro, Protobuf,
// BigQuery, etc and transform to validation types.  For example a "CountryCode" field is a string
// in the avro schema and we convert it to a scala case class with a CountryCode type that has a
// built in validator.  Elitzur will handle the logic to convert from the avro schema to the
// scala case class.  Then we use the use the built in validator to check which records are valid

object AvroBasic {

  // Age, NonNegativeLong and CountryCode are types that we validate
  // userFloat is a field which we choose to not validate.
  case class User(
                   userAge: Age,
                   userLong: NonNegativeLong,
                   userFloat: Float,
                   inner: InnerNested
                 )

  case class InnerNested(countryCode: CountryCode)

  // A MetricsReporter is needed to keep track of how many records are valid and invalid
  // For Scio use cases, one does not need to define a MetricsReporter
  implicit val metricsReporter: MetricsReporter = new MetricsReporter {
    val map : scala.collection.mutable.Map[String, Int] =
      scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    override def reportValid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.valid") += 1
    override def reportInvalid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.invalid") += 1
    override def toString: String = map.toString()
  }

  // Deserialized avro records.
  //scalastyle:off magic.number
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
  //scalastyle:on magic.number

  val c: AvroConverter[User] = implicitly[AvroConverter[User]]
  val v: Validator[User] = implicitly[Validator[User]]

  def main(args: Array[String]): Unit = {
    avroRecords
      // transform avro records to arbitrary case class with validation types
      // The AvroConverter is smart enough to transform string, longs, etc to Age, CountryCode,
      // and NonnegativeLong validation types with arbitrary levels of nesting in the avro schema
      .map(a => c.fromAvro(a, TestAvroTypes.SCHEMA$))
      // validate records
      .map(a => v.validateRecord(Unvalidated(a)))
      // map case classes to avro output
      .map(a => c.toAvro(a.forceGet, TestAvroOut.SCHEMA$))

    // See valid/invalid counts of fields which have been validated
    //scalastyle:off regex
    println(metricsReporter.toString)
    //scalastyle:on regex
  }
}
