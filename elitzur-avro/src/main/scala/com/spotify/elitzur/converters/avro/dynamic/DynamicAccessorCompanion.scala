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
package com.spotify.elitzur.converters.avro.dynamic

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.AvroElitzurConversionUtils.byteBufferToByteArray
import com.spotify.elitzur.validators.{
  BaseCompanion,
  BaseValidationType,
  SimpleCompanionImplicit,
  Validator
}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}
import java.{util => ju}
import collection.JavaConverters._

//scalastyle:off line.size.limit structural.type
class DynamicAccessorCompanion[T: TypeTag, LT <: BaseValidationType[T]: ClassTag: ({type L[x] = SimpleCompanionImplicit[T, x]})#L] extends Serializable {
//scalastyle:on line.size.limit structural.type

  private val companion: BaseCompanion[T, LT] =
    implicitly[SimpleCompanionImplicit[T, LT]].companion
  private[dynamic] val validationType: String = companion.validationType

  //scalastyle:off line.size.limit
  private[dynamic] def getValidator(modifier: Modifier)(implicit metricReporter: MetricsReporter): Validator[Any] = {
  //scalastyle:on line.size.limit
    modifier match {
      case Modifier.None => implicitly[Validator[LT]].asInstanceOf[Validator[Any]]
      case Modifier.Opt => implicitly[Validator[Option[LT]]].asInstanceOf[Validator[Any]]
      case Modifier.Seq => implicitly[Validator[Seq[LT]]].asInstanceOf[Validator[Any]]
      case Modifier.SeqOpt => implicitly[Validator[Seq[Option[LT]]]].asInstanceOf[Validator[Any]]
      case Modifier.OptSeq => implicitly[Validator[Option[Seq[LT]]]].asInstanceOf[Validator[Any]]
      case Modifier.OptSeqOpt =>
        implicitly[Validator[Option[Seq[Option[LT]]]]].asInstanceOf[Validator[Any]]
    }
  }

  private[dynamic] def getPreprocessorForValidator(modifier: Modifier): Any => Any = {
    def baseFn(v: Any): Any = parseAvro(v)
    def toOptionFn(v: Any, fn: Any => Any): Any = Option(v).map(fn)
    def toArrayFn(v: Any, fn: Any => Any): Any = v.asInstanceOf[ju.List[Any]].asScala.map(fn)

    (fieldValue: Any) => modifier match {
      case Modifier.None => baseFn(fieldValue)
      case Modifier.Opt => toOptionFn(fieldValue, baseFn)
      case Modifier.Seq => toArrayFn(fieldValue, baseFn)
      case Modifier.SeqOpt => toArrayFn(fieldValue, (v: Any) => toOptionFn(v, baseFn))
      case Modifier.OptSeq => toOptionFn(fieldValue, (v: Any) => toArrayFn(v, baseFn))
      case Modifier.OptSeqOpt =>
        toOptionFn(fieldValue, (v: Any) => toArrayFn(v, (e: Any) => toOptionFn(e, baseFn)))
    }
  }

  private def parseAvro(o: Any): Any =
    typeOf[T] match {
      case t if t =:= typeOf[String] => companion.parseUnsafe(o.toString)
      case t if t =:= typeOf[Array[Byte]] => companion.parseUnsafe(
        byteBufferToByteArray(o.asInstanceOf[java.nio.ByteBuffer]))
      case _ => companion.parseUnsafe(o)
    }
}

sealed trait Modifier { val name: String }
object Modifier {
  case object None extends Modifier {override final val name: String = ""}
  case object Opt extends Modifier {override final val name: String = "Option"}
  case object Seq extends Modifier {override final val name: String = "Seq"}
  case object SeqOpt extends Modifier {override final val name: String = "Seq.Option"}
  case object OptSeq extends Modifier {override final val name: String = "Option.Seq"}
  case object OptSeqOpt extends Modifier {override final val name: String = "Option.Seq.Option"}
  val modifiers: Array[Modifier] = Array(None, Opt, Seq, SeqOpt, OptSeq, OptSeqOpt)
}
