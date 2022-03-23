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

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroAccessorException._
import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroObjMapper
import com.spotify.elitzur.validators.{
  BaseCompanion,
  DynamicRecordValidator,
  Unvalidated,
  Validator
}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.{Failure, Try}

class DynamicAccessorValidator(validatorProperties: Array[RecordValidatorProperty], schema: Schema)
 (implicit metricsReporter: MetricsReporter) {
  final val className: String = this.getClass.getName

  // From the user provided input, create a parser that can extract a value from a record and apply
  // to it the the parsing rule defined in the companion object.
  private[elitzur] val fieldParsers: Array[DynamicFieldParser] = {
    val (successes, failures) = validatorProperties.map { r =>
        Try(new DynamicFieldParser(r, schema)) match {
          case Failure(e) => Failure(InvalidDynamicFieldException(e, r.accessorPath))
          case s => s
      }
    }.partition(_.isSuccess)

    if (!failures.isEmpty) {
      throw InvalidDynamicFieldException(failures.flatMap(_.failed.toOption), schema)
    }

    successes.flatMap(_.toOption)
  }

  // Create a record validator that consists of all the field validators returned above
  private val validator: DynamicRecordValidator = DynamicRecordValidator(
    fieldParsers.map(_.fieldValidator),
    fieldParsers.map(_.fieldLabel)
  )

  def validateRecord(avroRecord: GenericRecord): Unit = {
    val parseAllResult: Seq[Any] = fieldParsers.map(_.fieldParser(avroRecord))
    validator.validateRecord(Unvalidated(parseAllResult), outermostClassName = Some(className))
  }
}

class DynamicFieldParser(validatorProperty: RecordValidatorProperty, schema: Schema) {
  private[elitzur] val fieldLabel: String =
    s"${validatorProperty.accessorPath}:${validatorProperty.companion.validationType}"

  private[elitzur] val fieldValidator: Validator[Any] = validatorProperty.validator

  private val fieldAccessor: Any => Any = AvroObjMapper.getAvroFun(
    validatorProperty.accessorPath, schema)

  def fieldParser(avroRecord: GenericRecord): Any = {
    val fieldValue = fieldAccessor(avroRecord)
    validatorProperty.companion.parseUnsafe(fieldValue)
  }
}

case class RecordValidatorProperty(
  accessorPath: String, companion: BaseCompanion[_, _], validator: Validator[Any])
