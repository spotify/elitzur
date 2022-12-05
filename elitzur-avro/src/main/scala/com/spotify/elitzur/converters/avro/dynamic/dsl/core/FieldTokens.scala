package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.SchemaToAccessorOpsExceptionMsg._

case class FieldTokens(field: String, op: Option[String], rest: Option[String])

object FieldTokens {
  private val TOKEN = '.'
  private def strToOpt(str: String) :Option[String] = if (str.nonEmpty) Some(str) else None

  def apply(path: String): FieldTokens = {
    if (path.headOption.contains(TOKEN)) {
      val (fieldOps, rest) = path.drop(1).span(_ != TOKEN)
      val (field, op) = fieldOps.span(char => char.isLetterOrDigit || char == '_')
      FieldTokens(field, strToOpt(op), strToOpt(rest))
    } else {
      throw new InvalidDynamicFieldException(MISSING_TOKEN)
    }
  }
}
