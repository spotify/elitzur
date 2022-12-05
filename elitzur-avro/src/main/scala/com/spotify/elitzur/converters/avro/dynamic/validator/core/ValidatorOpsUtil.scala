package com.spotify.elitzur.converters.avro.dynamic.validator.core

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.{
  ArrayBaseAccessor,
  BaseAccessor,
  IndexAccessor,
  NullableBaseAccessor
}

import scala.annotation.tailrec

object ValidatorOpsUtil extends Serializable {

  @tailrec
  def toValidatorOp(
    accessorOps: List[BaseAccessor],
    modifiers: List[ValidatorOp] = List.empty[ValidatorOp]
  ): List[ValidatorOp] = {
    if (accessorOps.isEmpty) {
      modifiers
    } else {
      accessorOps.lastOption.get match {
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


