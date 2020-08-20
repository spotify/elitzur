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

import java.lang.{StringBuilder => JStringBuilder}

import com.spotify.elitzur.{DataInvalidException, MetricsReporter}
import magnolia._

import scala.collection.compat.immutable.ArraySeq

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final private[elitzur] case class DerivedValidator[T] private(caseClass: CaseClass[Validator, T])
                                                             (implicit reporter: MetricsReporter)
  extends Validator[T] {
  //scalastyle:off method.length cyclomatic.complexity
  override def validateRecord(a: PreValidation[T],
                              path: String = "",
                              outermostClassName: Option[String] = None,
                              config: ValidationRecordConfig = DefaultRecordConfig)
  : PostValidation[T] = {
    var atLeastOneValid = false
    var atLeastOneInvalid = false

    val counterClassName =
      if (outermostClassName.isEmpty) caseClass.typeName.full else outermostClassName.get
    val cs = caseClass.parameters
      .map{ p =>
        val deref = p.dereference(a.forceGet)
        if (!p.typeclass.isInstanceOf[IgnoreValidator[_]]) {
          val name = new JStringBuilder(path.length + p.label.length)
            .append(path).append(p.label).toString
          //TODO: This get wrapped in unvalidated seems unnecessary, how can we get rid of it
          val o = if (
             p.typeclass.isInstanceOf[FieldValidator[_]]
          ) {
            val v = p.typeclass.validateRecord(Unvalidated(deref))
            val validationType = p.typeclass.asInstanceOf[FieldValidator[_]].validationType

            val c: ValidationFieldConfig = config.m
              .getOrElse(name, DefaultFieldConfig).asInstanceOf[ValidationFieldConfig]
            if (v.isValid) {
              if (c != NoCounter) {
                reporter.reportValid(
                  counterClassName,
                  name,
                  validationType)
              }
            }
            else if (v.isInvalid) {
              if (c == ThrowException) {
                throw new DataInvalidException(
                  s"Invalid value ${v.forceGet.toString} found for field $path${p.label}")
              }
              if (c != NoCounter) {
                reporter.reportInvalid(
                  counterClassName,
                  name,
                  validationType
                )
              }
            }
            v
          } else {
            p.typeclass.validateRecord(
              Unvalidated(deref),
              new JStringBuilder(path.length + p.label.length + 1)
                .append(path).append(p.label).append(".").toString,
              Some(counterClassName),
              config
            )
          }

          if (o.isValid) {
            atLeastOneValid = true
          }
          else if (o.isInvalid) {
            atLeastOneInvalid = true
          }

          o.forceGet
        }
        else {
          deref
        }
      }

    val record = caseClass.rawConstruct(cs)

    if (atLeastOneInvalid){
      Invalid(record)
    }
    else if (atLeastOneValid) {
      Valid(record)
    }
    else {
      IgnoreValidation(record)
    }
  }

  override def shouldValidate: Boolean =
    caseClass.parameters.exists(p => p.typeclass.shouldValidate)
  //scalastyle:on method.length cyclomatic.complexity
}
