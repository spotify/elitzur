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

package com.spotify.elitzur.converters.avro.dynamic

import com.spotify.elitzur.validators.{BaseCompanion, BaseValidationType, Validator}

abstract case class QaasValidationCompanion(validatorIdentifier: String) {
  val validator: Validator[_]
  def validatorCheckParser: Any => Any
}

object QaasValidationCompanionImplicits {
  implicit class AugmentBaseCompanion[T, LT <: BaseValidationType[T]](c: BaseCompanion[T, LT]) {
    def parseAvroObj(x: Any): Any = c.parse(x.asInstanceOf[T])
  }
}