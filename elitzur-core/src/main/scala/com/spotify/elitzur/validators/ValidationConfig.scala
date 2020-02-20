/*
 * Copyright 2020 Spotify AB.
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
