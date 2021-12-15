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
package com.spotify.elitzur.validators

import com.spotify.elitzur.MetricsReporter


@SuppressWarnings(Array("org.wartremover.warts.Var"))
final private[elitzur] case class DynamicRecordValidator(validators: Array[Validator[Any]],
                                                         labels: Array[String])
                                                        (implicit reporter: MetricsReporter)
  extends Validator[Seq[Any]] {

  override def validateRecord(a: PreValidation[Seq[Any]],
                              path: String = "",
                              outermostClassName: Option[String] = None,
                              config: ValidationRecordConfig = DefaultRecordConfig)
  : PostValidation[Seq[Any]] = {
    val ps = a.forceGet.toArray
    val as = new Array[ValidatorAccessor[Any]](ps.length)
    var i = 0

    while (i < ps.length) {
      val value = ps(i)
      as.update(i, ValidatorAccessor(validators(i), value, labels(i))
        .asInstanceOf[ValidatorAccessor[Any]])
      i = i + 1
    }

    Validator.validationLoop(
      as,
      identity[Seq[Any]],
      outermostClassName.getOrElse(
        throw new Exception("A class name is required for Metrics Reporting")),
      path,
      config
    )
  }

  override def shouldValidate: Boolean = true
}
