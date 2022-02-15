package com.spotify.elitzur.converters.avro.qaas.utils

import org.apache.avro.generic.GenericRecord
import java.{util => ju}

import scala.collection.convert.Wrappers

/**
 * Below
 * @param combiner
 */
abstract class AvroObjWrapper(combiner: AvroWrapperOperator) {
  def fn: Any => Any
  val _combiner = combiner
  def +(that: AvroObjWrapper): CombinedOperation = CombinedOperation(
    combiner.combine(this, that), that._combiner)
}

case class NoopAvroObjWrapper() extends AvroObjWrapper(new NoopWrapperOperator) {
  override def fn: Any => Any = (o: Any) => o
}

case class GenericAvroObjWrapper(idx: Int, c: AvroWrapperOperator) extends AvroObjWrapper(c) {
  override def fn: Any => Any = (o: Any) => o.asInstanceOf[GenericRecord].get(idx)
}

case class UnionNullAvroObjWrapper(op: AvroObjWrapper, c: AvroWrapperOperator)
  extends AvroObjWrapper(c) {
  override def fn: Any => Any = (o: Any) => if (o == null) o else op.fn(o)
}

case class ArrayAvroObjWrapper[T <: ju.List[_]](
  innerFn: Any => Any, cast: CastOperationBase[T], c: AvroWrapperOperator,
  shouldFlatten: Boolean = false) extends AvroObjWrapper(c) {
  override def fn: Any => Any = (o: Any) => {
    val res = new ju.ArrayList[Any]
    if (shouldFlatten) {
      // need to revisit this portion
      cast.cast(o).forEach{ elem => res.add(innerFn(elem)) }
    } else {
      cast.cast(o).forEach{ elem => res.add(innerFn(elem)) }
    }
    res
  }

  def toFlatten: ArrayAvroObjWrapper[T] = this.copy(shouldFlatten = true)
}

case class CombinedOperation(newFn: Any => Any, c: AvroWrapperOperator) extends AvroObjWrapper(c) {
  override def fn: Any => Any = newFn
}

/**
 * Below
 * @tparam T
 */
trait CastOperationBase[T <: ju.List[_]] {
  def cast: Any => T = (o: Any) => o.asInstanceOf[T]
}

case class CastArrayListOperation() extends CastOperationBase[ju.ArrayList[_]]

case class CastSeqWrapperOperation() extends CastOperationBase[Wrappers.SeqWrapper[_]]
