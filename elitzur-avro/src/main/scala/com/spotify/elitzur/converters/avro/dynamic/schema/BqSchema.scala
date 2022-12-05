package com.spotify.elitzur.converters.avro.dynamic.schema

import com.google.api.services.bigquery.model.TableFieldSchema

import java.{util => ju}

case class BqSchema(fieldSchemas: ju.List[TableFieldSchema], fieldMode: BqField.Mode)

object BqSchema {
  def apply(fieldSchema: ju.List[TableFieldSchema]): BqSchema =
    BqSchema(fieldSchema, BqField.Required)
  def apply(fieldSchema: ju.List[TableFieldSchema], fieldModeStr: String): BqSchema = {
    val mode: BqField.Mode = fieldModeStr match {
      case BqField.Required.mode => BqField.Required
      case BqField.Repeated.mode => BqField.Repeated
      case BqField.Nullable.mode => BqField.Nullable
    }
    BqSchema(fieldSchema, mode)
  }
}

object BqField {
  final val REQUIRED_MODE = "REQUIRED"
  final val REPEATED_MODE = "REPEATED"
  final val NULLABLE_MODE = "NULLABLE"

  sealed trait Mode {val mode: String}
  case object Required extends Mode {override val mode: String = REQUIRED_MODE}
  case object Repeated extends Mode {override val mode: String = REPEATED_MODE}
  case object Nullable extends Mode {override val mode: String = NULLABLE_MODE}
}

