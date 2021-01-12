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

trait ValidationConfig extends Serializable

trait ValidationRecordConfig extends ValidationConfig {
  def fieldConfig(name: String): ValidationFieldConfig
}


class MapConfig(
  m: Map[String, ValidationFieldConfig],
  default: ValidationFieldConfig = DefaultFieldConfig
) extends ValidationRecordConfig {
  override def fieldConfig(name: String): ValidationFieldConfig = m.getOrElse(name, default)
}

case object DefaultRecordConfig extends MapConfig(Map())

object ValidationRecordConfig {
  def apply(s: (String, ValidationFieldConfig)*): ValidationRecordConfig = new MapConfig(Map(s:_*))
}

sealed trait ValidationFieldConfig extends ValidationConfig
case object ThrowException extends ValidationFieldConfig
case object NoCounter extends ValidationFieldConfig
case object DefaultFieldConfig extends ValidationFieldConfig
