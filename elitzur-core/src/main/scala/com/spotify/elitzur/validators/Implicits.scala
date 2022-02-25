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
package com.spotify.elitzur.validators

import com.spotify.elitzur.MetricsReporter

import scala.reflect.ClassTag

trait Implicits {
  //scalastyle:off line.size.limit
  import Validator._

  implicit def stringValidator(implicit reporter: MetricsReporter): FieldValidator[String] = new IgnoreValidator[String]
  implicit def longValidator(implicit reporter: MetricsReporter): FieldValidator[Long] = new IgnoreValidator[Long]
  implicit def doubleValidator(implicit reporter: MetricsReporter): FieldValidator[Double] = new IgnoreValidator[Double]
  implicit def booleanValidator(implicit reporter: MetricsReporter): FieldValidator[Boolean] = new IgnoreValidator[Boolean]
  implicit def arrayByteValidator(implicit reporter: MetricsReporter): FieldValidator[Array[Byte]] = new IgnoreValidator[Array[Byte]]
  implicit def floatValidator(implicit reporter: MetricsReporter): FieldValidator[Float] = new IgnoreValidator[Float]
  implicit def intValidator(implicit reporter: MetricsReporter): FieldValidator[Int] = new IgnoreValidator[Int]

  implicit def baseTypeValidator[T <: BaseValidationType[_]: ClassTag](implicit reporter: MetricsReporter): FieldValidator[T] = new BaseFieldValidator[T]
  implicit def optionTypeValidator[T <: BaseValidationType[_]: FieldValidator: ClassTag](implicit reporter: MetricsReporter): FieldValidator[Option[T]] = new OptionTypeValidator[T]
  implicit def statusTypeValidator[T <: BaseValidationType[_]: FieldValidator: ClassTag](implicit reporter: MetricsReporter): FieldValidator[ValidationStatus[T]] = new StatusTypeValidator[T]
  implicit def statusOptionTypeValidator[T <: BaseValidationType[_]: FieldValidator: ClassTag](implicit reporter: MetricsReporter): FieldValidator[ValidationStatus[Option[T]]] = new StatusOptionTypeValidator[T]
  implicit def wrappedValidator[T: Validator]: Validator[ValidationStatus[T]] = new WrappedValidator[T]
  implicit def optionValidator[T: Validator]: Validator[Option[T]] = new OptionValidator[T]
  implicit def dynamicTypeValidator[T <: DynamicValidationType[_, _, _]: ClassTag](implicit reporter: MetricsReporter): DynamicValidator[T] = new DynamicValidator[T]
  implicit def seqValidator[T: Validator: ClassTag]: Validator[Seq[T]] = wrapSeqLikeValidator(() => Seq.newBuilder[T])
  implicit def listValidator[T: Validator: ClassTag]: Validator[List[T]] = wrapSeqLikeValidator(() => List.newBuilder[T])
  implicit def arrayValidator[T: Validator: ClassTag]: Validator[Array[T]] = wrapSeqLikeValidator(() => Array.newBuilder[T])
  implicit def vectorValidator[T: Validator: ClassTag]: Validator[Vector[T]] = wrapSeqLikeValidator(() => Vector.newBuilder[T])
  //scalastyle:on line.size.limit

}
