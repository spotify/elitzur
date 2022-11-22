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

import com.spotify.elitzur.MetricsReporter
import magnolia1._


@SuppressWarnings(Array("org.wartremover.warts.Var"))
final private[elitzur] case class DerivedValidator[T] private(caseClass: CaseClass[Validator, T])
                                                             (implicit reporter: MetricsReporter)
  extends Validator[T] {
  override def validateRecord(a: PreValidation[T],
                              path: String = "",
                              outermostClassName: Option[String] = None,
                              config: ValidationRecordConfig = DefaultRecordConfig)
  : PostValidation[T] = {
    val ps = caseClass.parameters
    val as = new Array[ValidatorAccessor[Any]](ps.length)
    var i = 0

    // Loop through parameters once to dereference and avoid leaking magnolia types in APIs
    while (i < ps.length) {
      val p = ps(i)
      val deref = p.dereference(a.forceGet)
      as.update(i, ValidatorAccessor(p.typeclass, deref, p.label)
        .asInstanceOf[ValidatorAccessor[Any]])
      i = i + 1
    }
    Validator.validationLoop(
      as,
      caseClass.rawConstruct,
      if (outermostClassName.isEmpty) caseClass.typeName.full else outermostClassName.get,
      path,
      config
    )
  }

  override def shouldValidate: Boolean = {
    val ps = caseClass.parameters
    var i = 0
    var shouldValidate = false

    while (i < ps.length) {
      val p = ps(i)
      if (p.typeclass.shouldValidate) {
        shouldValidate = true
      }
      i = i + 1
    }
    shouldValidate
  }
}
