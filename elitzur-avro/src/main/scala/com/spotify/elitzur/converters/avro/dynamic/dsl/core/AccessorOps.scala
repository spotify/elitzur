/*
 * Copyright 2022 Spotify AB.
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
package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import java.{util => ju}

trait BaseAccessor {
  def fn: Any => Any
}

case class NoopAccessor() extends BaseAccessor {
  override def fn: Any => Any = (o: Any) => o
}

case class IndexAccessor(fieldFn: Any => Any) extends BaseAccessor {
  override def fn: Any => Any = fieldFn
}

trait InnerBaseAccessor extends BaseAccessor {
  val innerOps: List[BaseAccessor]
  private val accessorFns: Any => Any = AccessorOpsUtil.combineFns(innerOps)
  val innerFn: Any => Any = (o: Any) => accessorFns(o)
}

trait NullableBaseAccessor extends InnerBaseAccessor

case class NullableAccessor(
  fieldFn: Any => Any,
  innerOps: List[BaseAccessor]
) extends NullableBaseAccessor {
  override def fn: Any => Any = (o: Any) => {
    val fieldValue = fieldFn(o)
    if (fieldValue == null) null else innerFn(o)
  }
}

trait ArrayBaseAccessor extends InnerBaseAccessor

case class ArrayFlatmapAccessor(
  fieldFn: Any => Any,
  innerOps: List[BaseAccessor]
) extends ArrayBaseAccessor {
  override def fn: Any => Any = (o: Any) => {
    val innerFieldObj = fieldFn(o)
    val res = new ju.ArrayList[Any]
    innerFieldObj.asInstanceOf[ju.List[Any]].forEach(
      elem => {
        val innerVal = innerFn(elem)
        if (innerVal == null) {
          res.add(null)
        } else {
          innerVal.asInstanceOf[ju.List[Any]].forEach( x => res.add(x))
        }
      }
    )
    res
  }
}

case class ArrayMapAccessor(
  fieldFn: Any => Any,
  innerOps: List[BaseAccessor]
) extends ArrayBaseAccessor {
  override def fn: Any => Any = (o: Any) => {
    val innerFieldObj = fieldFn(o)
    val res = new ju.ArrayList[Any]
    innerFieldObj.asInstanceOf[ju.List[Any]].forEach(
      elem => {
        val innerVal = innerFn(elem)
        if (innerVal == null) res.add(null) else { res.add(innerFn(elem)) }
      }
    )
    res
  }
}

case class ArrayNoopAccessor(
  fieldFn: Any => Any,
  innerOps: List[BaseAccessor],
  flatten: Boolean
) extends ArrayBaseAccessor {
  override def fn: Any => Any = (o: Any) => IndexAccessor(fieldFn).fn(o)
}
