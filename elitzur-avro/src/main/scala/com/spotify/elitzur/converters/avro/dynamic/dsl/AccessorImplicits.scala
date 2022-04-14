package com.spotify.elitzur.converters.avro.dynamic.dsl

import org.apache.avro.Schema

import scala.annotation.tailrec

object AccessorImplicits {
  implicit class AccessorFunctionUtils(val ops: List[BaseAccessor]) {
    private[dsl] def combineFns: Any => Any =
      ops.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)

    @tailrec
    final private[dsl] def innerSchema: Option[Schema] = {
      ops.lastOption match {
        case Some(n: NullableAccessor) => n.innerOps.innerSchema
        case Some(a: ArrayMapAccessor) => a.innerOps.innerSchema
        case Some(a: ArrayFlatmapAccessor) => a.innerOps.innerSchema
        case Some(a: ArrayNoopAccessor) => Some(a.schema)
        case Some(i: IndexAccessor) => Some(i.schema)
        case _ => None
      }
    }

    private[dsl] def hasArray: Boolean = {
      ops.foldLeft(false)((accBoolean, currAccessor) => {
        val hasArrayAccessor = currAccessor match {
          case n: NullableAccessor => n.innerOps.hasArray
          case _: ArrayMapAccessor | _: ArrayFlatmapAccessor | _: ArrayNoopAccessor  => true
          case _ => false
        }
        accBoolean || hasArrayAccessor
      })
    }

    private[dsl] def hasNullable: Boolean = {
      ops.foldLeft(false)((accBoolean, currAccessor) => {
        val hasNullableAccessor = currAccessor match {
          case _: ArrayNoopAccessor => false
          case _: NullableAccessor => true
          case a: ArrayMapAccessor => a.innerOps.hasNullable
          case a: ArrayFlatmapAccessor => a.innerOps.hasNullable
          case _ => false
        }
        accBoolean || hasNullableAccessor
      })
    }
  }
}
