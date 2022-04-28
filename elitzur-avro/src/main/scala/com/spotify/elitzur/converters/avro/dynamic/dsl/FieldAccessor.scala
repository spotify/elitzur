package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.{
  ArrayValidatorOp,
  OptionValidatorOp,
  ValidatorOp
}

import scala.annotation.tailrec

case class FieldAccessor(accessors: List[BaseAccessor]) extends Serializable {
  def combineFns: Any => Any =
    accessors.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)

  def toValidatorOp: List[ValidatorOp] = toValidatorOp(this.accessors)

  @tailrec
  private def toValidatorOp(
    ops: List[BaseAccessor],
    modifiers: List[ValidatorOp] = List.empty[ValidatorOp]
  ): List[ValidatorOp] = {
    if (ops.isEmpty) {
      List.empty[ValidatorOp]
    } else {
      ops.lastOption.get match {
        case n: NullableBaseAccessor =>
          // A sequence of options can be reduce to a single option operation
          if (modifiers.lastOption.contains(OptionValidatorOp)) {
            toValidatorOp(n.innerOps, modifiers)
          } else {
            toValidatorOp(n.innerOps, modifiers :+ OptionValidatorOp)
          }
        case a: ArrayBaseAccessor =>
          // The DSL will flatten nested arrays into a single array. The first instance of an
          // array is captured below.
          if (modifiers.contains(ArrayValidatorOp)) {
            toValidatorOp(a.innerOps, modifiers)
          } else {
            toValidatorOp(a.innerOps, modifiers :+ ArrayValidatorOp)
          }
        case _: IndexAccessor => modifiers
      }
    }
  }
}
