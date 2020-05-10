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
package com.spotify.elitzur.scio

import com.spotify.elitzur.validators.{
  PostValidation, Unvalidated, ValidationRecordConfig, Validator
}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object ValidatorDoFns {

  class ValidatorDoFn[T: Coder](vr: Validator[T],
                                config: ValidationRecordConfig = ValidationRecordConfig())
    extends DoFn[T, T] with Serializable {
    @ProcessElement
    def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val e = c.element()
      c.output(vr.validateRecord(Unvalidated(e), config = config).forceGet)
    }
  }

  class ValidatorDoFnWithResult[T: Coder](vr: Validator[T],
                                          config: ValidationRecordConfig = ValidationRecordConfig())
    extends DoFn[T, PostValidation[T]] with Serializable {
    @ProcessElement
    def processElement(c: DoFn[T, PostValidation[T]]#ProcessContext): Unit = {
      val e = c.element()
      c.output(vr.validateRecord(Unvalidated(e), config = config))
    }
  }

}
