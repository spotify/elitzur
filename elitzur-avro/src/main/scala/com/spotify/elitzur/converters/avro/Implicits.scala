/*
 * Copyright 2020 Spotify AB.
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
package com.spotify.elitzur.converters.avro

import com.spotify.scio.coders.Coder
import org.apache.avro.Schema
import org.apache.avro.generic.GenericContainer
import enumeratum._

import scala.language.reflectiveCalls
import scala.reflect.ClassTag

trait Implicits {
  import AvroConverter._
  import com.spotify.elitzur.validators._

  implicit val intT: PrimitiveConverter[Int] = new PrimitiveConverter[Int]
  implicit val longT: PrimitiveConverter[Long] = new PrimitiveConverter[Long]
  implicit val doubleT: PrimitiveConverter[Double] = new PrimitiveConverter[Double]
  implicit val floatT: PrimitiveConverter[Float] = new PrimitiveConverter[Float]
  implicit val boolT: PrimitiveConverter[Boolean] = new PrimitiveConverter[Boolean]
  implicit val arrBT: AvroConverter[Array[Byte]] = new AvroConverter[Array[Byte]] {
    override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): Array[Byte] = {
      val bb = v.asInstanceOf[java.nio.ByteBuffer]
      AvroElitzurConversionUtils.byteBufferToByteArray(bb)
    }

    override def toAvro(v: Array[Byte], schema: Schema): Any = {
      java.nio.ByteBuffer.wrap(v)
    }

    override def toAvroDefault(v: Array[Byte], defaultGenericContainer: GenericContainer): Any = {
      java.nio.ByteBuffer.wrap(v)
    }
  }
  // we  can't use a primitive converter here because we need to be able to convert strings of the
  // type: org.apache.avro.util.Utf8 (a subclass of Charsequence) which cannot be cast to a string
  implicit val stringT: AvroConverter[String] = new AvroConverter[String] {
    override def fromAvro(v: Any, schema: Schema, doc: Option[String]): String = v.toString

    override def toAvro(v: String, schema: Schema): Any = v

    override def toAvroDefault(v: String, defaultGenericContainer: GenericContainer): Any = v
  }

  //scalastyle:off line.size.limit structural.type
  implicit def simpleTypeConverter[LT <: BaseValidationType[T]: ({type L[x] = SimpleCompanionImplicit[T, x]})#L,T: AvroConverter]
  : AvroConverter[LT] =
    new AvroSimpleTypeConverter[LT, T]

  implicit def dynamicTypeConverter[LT <: DynamicValidationType[T, _, LT]: ({type L[x] = DynamicCompanionImplicit[T, _, x]})#L, T: AvroConverter]
  : AvroConverter[LT] =
    new AvroDynamicTypeConverter[LT, T]
  //scalastyle:on line.size.limit structural.type

  implicit def validationTypeOptionConverter[T <: BaseValidationType[_]: AvroConverter]
  : AvroConverter[Option[T]] =
    new AvroOptionConverter[T]

  implicit def wrappedValidationTypeConverter[T <: BaseValidationType[_]: AvroConverter]
  : AvroConverter[ValidationStatus[T]] =
    new AvroStatusConverter[T]

  implicit def statusOptionEncryptionValidator[T <: BaseValidationType[_]: AvroConverter]
  : AvroConverter[ValidationStatus[Option[T]]] =
    new AvroStatusOptionConverter[T]

  implicit def optionConverter[T: AvroConverter]: AvroConverter[Option[T]] =
    new OptionConverter[T]

  implicit def wrappedValidationConverter[T: AvroConverter]: AvroConverter[ValidationStatus[T]] =
    new AvroWrappedValidationConverter[T]

  implicit def seqConverter[T: AvroConverter: Coder: ClassTag]: AvroConverter[Seq[T]] =
    new AvroSeqConverter[T, Seq](() => Seq.newBuilder[T])

  implicit def listConverter[T: AvroConverter: Coder: ClassTag]: AvroConverter[List[T]] =
    new AvroSeqConverter[T, List](() => List.newBuilder[T])

  implicit def vectorConverter[T: AvroConverter: Coder: ClassTag]: AvroConverter[Vector[T]] =
    new AvroSeqConverter[T, Vector](() => Vector.newBuilder[T])

  implicit def arrayConverter[T: AvroConverter: Coder: ClassTag]: AvroConverter[Array[T]] =
    new AvroSeqConverter[T, Array](() => Array.newBuilder[T])

  implicit def enumConverter[T <: enumeratum.EnumEntry: Enum]: AvroConverter[T] =
    new AvroEnumConverter[T]

}
