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
package com.spotify.elitzur.converters.avro.dynamic.validator.core

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.AvroElitzurConversionUtils.byteBufferToByteArray
import com.spotify.elitzur.converters.avro.dynamic.schema.SchemaType
import com.spotify.elitzur.validators.{
  BaseCompanion,
  BaseValidationType,
  SimpleCompanionImplicit,
  Validator
}
import org.apache.avro.Schema

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.{TypeTag, typeOf}

//scalastyle:off line.size.limit structural.type
class DynamicAccessorCompanion[T: TypeTag, LT <: BaseValidationType[T]: ClassTag: ({type L[x] = SimpleCompanionImplicit[T, x]})#L] extends Serializable {
//scalastyle:on line.size.limit structural.type

  private val companion: BaseCompanion[T, LT] =
    implicitly[SimpleCompanionImplicit[T, LT]].companion
  private[dynamic] val validationType: String = companion.validationType
  private def parseUnsafe(v: Any): Any = companion.parse(v.asInstanceOf[T])

  private def preProcessParser[R: SchemaType: TypeTag]: Any => Any = {
    typeOf[R] match {
      case r if r =:= typeOf[Schema] =>
        typeOf[T] match {
          // String in Avro can be stored as org.apache.avro.util.Utf8 (a subclass of Charsequence)
          // which cannot be cast to String as-is. The toString method is added to ensure casting.
          case t if t =:= typeOf[String] => (v: Any) => v.toString
          // ByteBuffer in Avro to be converted into Array[Byte] which is the the format that
          // Validation type expects the input to be in.
          case t if t =:= typeOf[Array[Byte]] =>
            (v: Any) => byteBufferToByteArray(v.asInstanceOf[java.nio.ByteBuffer])
          case _ => (v: Any) => v
        }
      case _ => (v: Any) => v
    }
  }

  // TODO: Optimize the method below by introducing changes to Elitzur-Core to allow non-implicit
  //  driven wiring of Validators
  //scalastyle:off line.size.limit
  private[dynamic] def getValidator(modifiers: List[ValidatorOp])(implicit m: MetricsReporter): Validator[Any] = {
  //scalastyle:on line.size.limit
    modifiers match {
      case Nil => implicitly[Validator[LT]].asInstanceOf[Validator[Any]]
      case OptionValidatorOp :: Nil => implicitly[Validator[Option[LT]]]
        .asInstanceOf[Validator[Any]]
      case ArrayValidatorOp :: Nil => implicitly[Validator[Seq[LT]]]
        .asInstanceOf[Validator[Any]]
      case ArrayValidatorOp :: OptionValidatorOp :: Nil => implicitly[Validator[Seq[Option[LT]]]]
        .asInstanceOf[Validator[Any]]
      case OptionValidatorOp :: ArrayValidatorOp :: Nil => implicitly[Validator[Option[Seq[LT]]]]
        .asInstanceOf[Validator[Any]]
      case OptionValidatorOp :: ArrayValidatorOp :: OptionValidatorOp :: Nil =>
        implicitly[Validator[Option[Seq[Option[LT]]]]].asInstanceOf[Validator[Any]]
      case _ => throw new Exception(s"Unsupported validator operation: ${modifiers.mkString(",")}")
    }
  }

  /**
   *   The expected input for the dynamic Elitzur is in the form ".path.to.field:thisValidator".
   *   The first part of the input (".path.to.field") is used by the DSL to generate an output
   *   of Any. Before this output can be mapped to the Elitzur-Core's Validation loop, the output
   *   should be wrapped inside of the validator object 'thisValidator'. A few example of what
   *   the method below could output includes: thisValidator(Any), Some(thisValidator(Any)) and
   *   List(thisValidator(Any)).
   */
  private[dynamic] def typedFieldValueFn[R: SchemaType: TypeTag](
    modifiers: List[ValidatorOp]
  ): Any => Any = {
    val schemaBasedParser: Any => Any = preProcessParser[R]
    val parseFieldValue: Any => Any = (v: Any) => parseUnsafe(schemaBasedParser(v))
    modifiers.reverse.foldLeft(parseFieldValue)((a, c) => (v: Any) => c.preprocessorOp(v, a))
  }
}
