package com.spotify.elitzur.converters.avro.dynamic.dsl

import com.spotify.elitzur.converters.avro.dynamic.{ArrayValidatorOp, ValidatorOp, OptionValidatorOp}

import scala.annotation.tailrec

object Implicits {
  implicit class AccessorOps(val accessors: List[BaseAccessor]) {
    def combineFns: Any => Any =
      accessors.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)

    def toValidatorOp: List[ValidatorOp] = accessors.toValidatorOp(List.empty[ValidatorOp])

    @tailrec
    private def toValidatorOp(modifiers: List[ValidatorOp]): List[ValidatorOp] = {
      if (accessors.isEmpty) {
        List.empty[ValidatorOp]
      } else {
        accessors.lastOption.get match {
          case n: NullableAccessor =>
            // A sequence of options can be reduce to a single option operation
            if (modifiers.lastOption.contains(OptionValidatorOp)) {
              n.innerOps.toValidatorOp(modifiers)
            } else {
              n.innerOps.toValidatorOp(modifiers :+ OptionValidatorOp)
            }
          case a: ArrayBaseAccessor =>
            // The DSL will flatten nested arrays into a single array. The first instance of an
            // array is captured below.
            if (modifiers.contains(ArrayValidatorOp)) {
              a.innerOps.toValidatorOp(modifiers)
            } else {
              a.innerOps.toValidatorOp(modifiers :+ ArrayValidatorOp)
            }
          case _: IndexAccessor => modifiers
        }
      }
    }
  }

}