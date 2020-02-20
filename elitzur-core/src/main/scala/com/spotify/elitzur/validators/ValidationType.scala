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

import scala.language.{reflectiveCalls, higherKinds, existentials}
import scala.util.Try

trait BaseValidationType[T] {
  def checkValid: Boolean

  def data: T

  override def toString: String = data.toString
}

//scalastyle:off line.size.limit structural.type
/**
 * Validation type that allows dynamic (runtime) setting of an arg that is used in the validation
 * function. This accepts any arbitrary args, however has implicit state (whether the arg is set)
 * so we recommend using this sparingly.
 *
 * @tparam T The field data type
 * @tparam U The argument type needed for validation
 * @tparam A F-bounded polymorphism type
 */
abstract class DynamicValidationType[T, U, A <: DynamicValidationType[T, U, A]: ({type L[x] = DynamicCompanionImplicit[T, U, x]})#L]
  extends BaseValidationType[T] {

  private[elitzur] def arg: Option[U]

  def setArg(a: U): A = {
    implicitly[DynamicCompanionImplicit[T, U, A]].companion.parseWithArg(data, a)
  }
}
//scalastyle:on line.size.limit structural.type
