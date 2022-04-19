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
import com.spotify.elitzur.converters.avro.dynamic.DynamicAccessorException.InvalidModifierException
import com.spotify.elitzur.converters.avro.AvroElitzurConversionUtils.byteBufferToByteArray
import com.spotify.elitzur.validators.{
  BaseCompanion,
  BaseValidationType,
  SimpleCompanionImplicit,
  Validator
}

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeOf

import java.{util => ju}
import collection.JavaConverters._

//scalastyle:off line.size.limit structural.type
class DynamicAccessorCompanion[T: TypeTag, LT <: BaseValidationType[T]: ClassTag: ({type L[x] = SimpleCompanionImplicit[T, x]})#L](
  implicit metricReporter: MetricsReporter) {
//scalastyle:on line.size.limit structural.type

  private val companion: BaseCompanion[T, LT] =
    implicitly[SimpleCompanionImplicit[T, LT]].companion
  private[dynamic] val validationType: String = companion.validationType

  private[dynamic] def getModifier(input: String): Modifier = {
    if (!input.contains("[")) {
      NoModifier
    } else {
      val validatorModifierStrArray: Array[String] = input.split("\\[").dropRight(1)

      validatorModifierStrArray.mkString(".") match {
        case "Option" => OptModifier
        case "Seq" => SeqModifier
        case "Seq.Option" => SeqOptModifier
        case "Option.Seq" => OptSeqModifier
        case "Option.Seq.Option" => OptSeqOptModifier
        case _ => throw new InvalidModifierException("not supported modifier")
      }
    }
  }

  private[dynamic] def getValidator(modifier: Modifier): Validator[Any] = {
    modifier match {
      case NoModifier => implicitly[Validator[LT]].asInstanceOf[Validator[Any]]
      case OptModifier => implicitly[Validator[Option[LT]]].asInstanceOf[Validator[Any]]
      case SeqModifier => implicitly[Validator[Seq[LT]]].asInstanceOf[Validator[Any]]
      case SeqOptModifier => implicitly[Validator[Seq[Option[LT]]]].asInstanceOf[Validator[Any]]
      case OptSeqModifier => implicitly[Validator[Option[Seq[LT]]]].asInstanceOf[Validator[Any]]
      case OptSeqOptModifier =>
        implicitly[Validator[Option[Seq[Option[LT]]]]].asInstanceOf[Validator[Any]]
    }
  }

  private[dynamic] def getPreprocessorForValidator(modifier: Modifier): Any => Any = {
    def baseFn(v: Any): Any = parseAvro(v)
    def toOptionFn(v: Any, fn: Any => Any): Any = Option(v).map(fn)
    def toArrayFn(v: Any, fn: Any => Any): Any = v.asInstanceOf[ju.List[Any]].asScala.map(fn)

    (fieldValue: Any) => modifier match {
      case NoModifier => baseFn(fieldValue)
      case OptModifier => toOptionFn(fieldValue, baseFn)
      case SeqModifier => toArrayFn(fieldValue, baseFn)
      case SeqOptModifier => toArrayFn(fieldValue, (v: Any) => toOptionFn(v, baseFn))
      case OptSeqModifier => toOptionFn(fieldValue, (v: Any) => toArrayFn(v, baseFn))
      case OptSeqOptModifier =>
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

sealed trait Modifier
private case object NoModifier extends Modifier
private case object OptModifier extends Modifier
private case object SeqModifier extends Modifier
private case object SeqOptModifier extends Modifier
private case object OptSeqModifier extends Modifier
private case object OptSeqOptModifier extends Modifier

object DynamicAccessorException {
  class InvalidModifierException(modifier: String) extends Exception(
    s"""
      |Invalid input of $modifier detected for the validation setting. The available validation
      |settings for a given ValidationType, VT, are: VT, Option[VT], Seq[VT], Seq[Option[VT]],
      |Option[Seq[VT]] and Option[Seq[Option[VT]]]
      |""".stripMargin
  )
}
