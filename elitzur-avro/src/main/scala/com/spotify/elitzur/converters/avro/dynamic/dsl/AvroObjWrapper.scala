package com.spotify.elitzur.converters.avro.dynamic.dsl

import org.apache.avro.generic.GenericRecord
import java.{util => ju}

trait BaseFilter {
  def fn: Any => Any
}

case class NoopFilter() extends BaseFilter {
  def fn: Any => Any = (o: Any) => o
}

case class IndexFilter(idx: Int) extends BaseFilter {
  override def fn: Any => Any = (o: Any) => o.asInstanceOf[GenericRecord].get(idx)
}

case class NullableFilter(idx: Int, innerFn: Any => Any) extends BaseFilter {
  override def fn: Any => Any = (o: Any) => {
    val innerAvroObj = o.asInstanceOf[GenericRecord].get(idx)
    if (innerAvroObj == null) null else innerFn(o)
  }
}

case class ArrayFilter(idx: Int, innerFn: Any => Any, flatten: Boolean)
  extends BaseFilter {
  override def fn: Any => Any = (o: Any) => {
    val innerAvroObj = o.asInstanceOf[GenericRecord].get(idx)
    val res = new ju.ArrayList[Any]
    innerAvroObj.asInstanceOf[ju.List[Any]].forEach(elem => res.add(innerFn(elem)))
    if (flatten) { flattenArray(res) } else { res }
  }

  private def flattenArray(list: ju.ArrayList[Any]): ju.ArrayList[Any] = {
    val flattenRes = new ju.ArrayList[Any]
    list.forEach(elem => elem.asInstanceOf[ju.ArrayList[Any]].forEach( x => flattenRes.add(x) ))
    flattenRes
  }
}