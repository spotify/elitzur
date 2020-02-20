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
