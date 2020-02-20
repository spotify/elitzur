package com.spotify.elitzur

import com.spotify.elitzur._
import com.spotify.elitzur.converters.avro.AvroConverter
import com.spotify.elitzur.schemas.{TestAvroOut, TestAvroTypes}
import com.spotify.elitzur.validators._
import com.spotify.elitzur.converters.avro._
import com.spotify.ratatool.scalacheck._
import com.spotify.elitzur.Companions._
import com.spotify.elitzur.validators.{Unvalidated, Validator}
import org.scalacheck._


object ProfilingAvro {
  case class TestAvro(
                       userAge: AgeExample,
                       userLong: NonNegativeLongExample,
                       userFloat: Float,
                       inner: InnerNested
                     )

  case class InnerNested(countryCode: CountryCodeExample)

  implicit val metricsReporter: MetricsReporter = new MetricsReporter {
    val map : scala.collection.mutable.Map[String, Int] =
      scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    override def reportValid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.valid") += 1
    override def reportInvalid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.invalid") += 1
    override def toString: String = map.toString()
  }

  //scalastyle:off magic.number
  val avroRecords: Seq[TestAvroTypes] = Gen.listOfN(1000, avroOf[TestAvroTypes]).sample.get
  //scalastyle:on magic.number

  val c: AvroConverter[TestAvro] = implicitly[AvroConverter[TestAvro]]
  val v: Validator[TestAvro] = implicitly[Validator[TestAvro]]


  def main(args: Array[String]): Unit = {
    avroRecords
      .map(a => c.fromAvro(a, TestAvroTypes.SCHEMA$))
      .map(a => v.validateRecord(Unvalidated(a)))
      .map(a => c.toAvro(a.forceGet, TestAvroOut.SCHEMA$))
  }
}
