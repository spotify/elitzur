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
import com.spotify.elitzur.validators.Validator.wrapSeqLikeValidatorBase
import com.spotify.elitzur.validators.{BaseCompanion, BaseValidationType, FieldValidator, OptionTypeBaseValidator, SimpleCompanionImplicit, Validator}

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

  private def parseAvro(o: Any): Any =
    typeOf[T] match {
      // String in Avro can be stored as org.apache.avro.util.Utf8 (a subclass of Charsequence)
      // which cannot be cast to String as-is. The toString method is added to ensure casting.
      case t if t =:= typeOf[String] => companion.parseUnsafe(o.toString)
      // ByteBuffer in Avro to be converted into Array[Byte] which is the the format that Validation
      // type expects the input the input to be in.
      case t if t =:= typeOf[Array[Byte]] => companion.parseUnsafe(
        byteBufferToByteArray(o.asInstanceOf[java.nio.ByteBuffer]))
      case _ => companion.parseUnsafe(o)
    }

  //scalastyle:off line.size.limit
  private[dynamic] def getValidator(modifiers: List[ValidatorOp])(implicit m: MetricsReporter): Validator[Any] = {
  //scalastyle:on line.size.limit
    val baseValidator = implicitly[Validator[LT]].asInstanceOf[Validator[Any]]
    modifiers.reverse.foldLeft(baseValidator)((a, c) => c.validatorOp(a))
  }

  private[dynamic] def getPreprocessorForValidator(modifiers: List[ValidatorOp]): Any => Any = {
    val baseFn: Any => Any = (v: Any) => parseAvro(v)
    modifiers.reverse.foldLeft(baseFn)((a, c) => (v: Any) => c.preprocessorOp(v, a))
  }
}


trait ValidatorOp {
  def preprocessorOp(v: Any, fn: Any => Any): Any
  def validatorOp[LT <: BaseValidationType[_]](
    innerValidator: Validator[Any]
  )(implicit m: MetricsReporter): Validator[Any]
}

object OptionValidatorOp extends ValidatorOp {
  def preprocessorOp(v: Any, fn: Any => Any): Any = Option(v).map(fn)
  def validatorOp[LT <: BaseValidationType[_]](
    innerValidator: Validator[Any]
  )(implicit m: MetricsReporter): Validator[Any] =
    new OptionTypeBaseValidator[LT](innerValidator.asInstanceOf[FieldValidator[LT]])
      .asInstanceOf[Validator[Any]]
}

case object ArrayValidatorOp extends ValidatorOp {
  def preprocessorOp(v: Any, fn: Any => Any): Any = v.asInstanceOf[ju.List[Any]].asScala.map(fn)
  def validatorOp[LT](
    innerValidator: Validator[Any]
  )(implicit m: MetricsReporter): Validator[Any] =
    wrapSeqLikeValidatorBase(innerValidator, () => Seq.newBuilder[Any]).asInstanceOf[Validator[Any]]
}