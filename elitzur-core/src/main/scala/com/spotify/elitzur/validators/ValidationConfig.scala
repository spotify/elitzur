package com.spotify.elitzur.validators

trait ValidationConfig
trait ValidationFieldConfig extends ValidationConfig

trait ValidationRecordConfig extends ValidationConfig {
  def m: Map[String, ValidationConfig]
}

case class MapConfig(m: Map[String, ValidationConfig]) extends ValidationRecordConfig

case object DefaultRecordConfig extends ValidationRecordConfig {
  override def m: Map[String, ValidationConfig] = Map()
}

object ValidationRecordConfig {
  def apply(s: (String, ValidationConfig)*): ValidationRecordConfig =
    MapConfig(Map(s:_*))
}

case object ThrowException extends ValidationFieldConfig
case object NoCounter extends ValidationFieldConfig
case object DefaultFieldConfig extends ValidationFieldConfig
