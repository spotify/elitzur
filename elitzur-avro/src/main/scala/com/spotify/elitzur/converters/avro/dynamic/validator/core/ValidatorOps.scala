package com.spotify.elitzur.converters.avro.dynamic.validator.core

import java.{util => ju}
import collection.JavaConverters._

trait ValidatorOp extends Serializable {
  def preprocessorOp(v: Any, fn: Any => Any): Any
}

object OptionValidatorOp extends ValidatorOp {
  def preprocessorOp(v: Any, fn: Any => Any): Any = Option(v).map(fn)
}

case object ArrayValidatorOp extends ValidatorOp {
  def preprocessorOp(v: Any, fn: Any => Any): Any = v.asInstanceOf[ju.List[Any]].asScala.map(fn)
}
