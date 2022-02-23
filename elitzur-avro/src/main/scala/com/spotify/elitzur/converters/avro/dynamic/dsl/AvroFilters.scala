/*
 * Copyright 2021 Spotify AB.
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

case class ArrayFilter(idx: Int, innerFn: Any => Any, flatten: Boolean, isLastArray: Boolean)
  extends BaseFilter {
  override def fn: Any => Any = (o: Any) => {
    val innerAvroObj = o.asInstanceOf[GenericRecord].get(idx)
    val res = new ju.ArrayList[Any]

    (flatten, isLastArray) match {
      case (true, true) | (false, false) =>
        innerAvroObj.asInstanceOf[ju.List[Any]].forEach(elem => res.add(innerFn(elem)))
        res
      case (true, false) =>
        innerAvroObj.asInstanceOf[ju.List[Any]].forEach(
          elem => innerFn(elem).asInstanceOf[ju.List[Any]].forEach( x => res.add(x)))
        res
      case (false, true) =>
        innerAvroObj
    }
  }
}