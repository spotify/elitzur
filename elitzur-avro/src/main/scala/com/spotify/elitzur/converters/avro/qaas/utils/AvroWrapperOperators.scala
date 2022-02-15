package com.spotify.elitzur.converters.avro.qaas.utils

import scala.collection.convert.Wrappers
import java.{util => ju}

trait AvroWrapperOperator {
  def combine(`this`: AvroObjWrapper, that: AvroObjWrapper): Any => Any
}

class DefaultWrapperOperator extends AvroWrapperOperator {
  override def combine(`this`: AvroObjWrapper, that: AvroObjWrapper): Any => Any =
    that match {
      case _: ArrayAvroObjWrapper[_] => throw new Exception("not covered")
      case _ => `this`.fn andThen that.fn
    }
}

class ArrayWrapperOperator extends AvroWrapperOperator {
  override def combine(`this`: AvroObjWrapper, that: AvroObjWrapper): Any => Any =
    that match {
      case x: ArrayAvroObjWrapper[_] => `this`.fn andThen x.toFlatten.fn
      case _ => throw new Exception("not covered")
    }
}

class NoopWrapperOperator extends AvroWrapperOperator {
  def combine(`this`: AvroObjWrapper, that: AvroObjWrapper): Any => Any = `this`.fn
}

class FlattenWrapperOperator extends AvroWrapperOperator {
  def combine(`this`: AvroObjWrapper, that: AvroObjWrapper): Any => Any = {
    (o: Any) =>
      val res = new ju.ArrayList[Any]
      `this`.fn(o) match {
        case e: ju.ArrayList[_] => e.forEach {
          case elems: ju.ArrayList[_] => elems.forEach( x => res.add(x) )
          case elems: Wrappers.SeqWrapper[_] => elems.forEach( x => res.add(x) )
        }
        case _ => throw new Exception("not covered")
      }
      res
  }
}

object OperatorUtils {
  def matchMethod(str: String): AvroWrapperOperator = {
    str match {
      case "." => new DefaultWrapperOperator
      case "[]." => new ArrayWrapperOperator
    }
  }

  def matchEndMethod(str: String): AvroWrapperOperator = {
    if (str.isEmpty) {
      new NoopWrapperOperator
    } else {
      str match {
        case "[]" => new FlattenWrapperOperator
      }
    }
  }
}