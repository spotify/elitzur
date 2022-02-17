package com.spotify.elitzur.converters.avro.dynamic.dsl

import org.apache.avro.generic.GenericRecord
import java.{util => ju}

trait BaseOperator {
  def fn: Any => Any
}

case class NoopOperator() extends BaseOperator {
  def fn: Any => Any = (o: Any) => o
}

case class GenericRecordOperator(idx: Int) extends BaseOperator {
  override def fn: Any => Any = (o: Any) => o.asInstanceOf[GenericRecord].get(idx)
}

case class NullableOperator(op: BaseOperator) extends BaseOperator {
  override def fn: Any => Any = (o: Any) => if (o == null) o else op.fn(o)
}

case class MapOperator(idx: Int, mapKey: Option[String]) extends BaseOperator {
  override def fn: Any => Any = (o: Any) => {
    val innerAvroObj = o.asInstanceOf[GenericRecord].get(idx)
    if (mapKey.isDefined) {
      innerAvroObj.asInstanceOf[ju.Map[CharSequence, Any]].get(mapKey.get)
    } else {
      innerAvroObj.asInstanceOf[ju.Map[CharSequence, Any]]
    }
  }
}

case class ArrayOperator(idx: Int, ops: List[BaseOperator], flatten: Boolean)
  extends BaseOperator {
  override def fn: Any => Any = (o: Any) => {
    val innerAvroObj = o.asInstanceOf[GenericRecord].get(idx)
    val res = new ju.ArrayList[Any]
    innerAvroObj.asInstanceOf[ju.List[Any]].forEach(elem => res.add(composedFn.get(elem)))
    if (flatten) { flattenArray(res) } else { res }
  }

  private def flattenArray(list: ju.ArrayList[Any]): ju.ArrayList[Any] = {
    val flattenRes = new ju.ArrayList[Any]
    list.forEach(elem => elem.asInstanceOf[ju.ArrayList[Any]].forEach( x => flattenRes.add(x) ))
    flattenRes
  }

  private val composedFn: Option[Any => Any] = ops.map(_.fn).reduceLeftOption((f, g) => f andThen g)
}