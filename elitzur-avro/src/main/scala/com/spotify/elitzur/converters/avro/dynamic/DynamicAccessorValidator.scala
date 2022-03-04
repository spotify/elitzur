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
import com.spotify.elitzur.converters.avro.dynamic.dsl.AvroObjMapper
import com.spotify.elitzur.validators.{DynamicRecordValidator, Unvalidated, Validator}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.util.{Failure, Success, Try}

class DynamicAccessorValidator(
  recordAccessorWithValidator: Array[(String, DynamicValidationCompanion)],
  schema: Schema
)(implicit val metricsReporter: MetricsReporter) {

  final val className: String = this.getClass.getName

  // From the user provided input, create a parser that can extract a value from a record and apply
  // to it the the parsing rule defined in the companion object.
  private[elitzur] val fieldParsers: Array[DynamicFieldParser] = {
    recordAccessorWithValidator.map { case (accessorPath, companion) =>
      Try(DynamicFieldParser(accessorPath, companion, schema)) match {
        case Success(s) => s
        case Failure(e) => throw new Exception(s"""
          |Unable to instantiate ${DynamicFieldParser.getClass.getName} given inputs
          |$accessorPath and ${companion.getClass.getName}.""".stripMargin, e)
      }
    }
  }

  // Create dynamic validator with validators and labels set in the same order as fieldParsers
  private[elitzur] val validator: DynamicRecordValidator = DynamicRecordValidator(
    fieldParsers.map(_.companion.validator).asInstanceOf[Array[Validator[Any]]],
    fieldParsers.map(_.label)
  )

  def validateRecord(avroRecord: GenericRecord): Unit = {
    val parseAllResult: Seq[Any] = fieldParsers.map(_.dynamicParser(avroRecord))
    validator.validateRecord(Unvalidated(parseAllResult), outermostClassName = Some(className))
  }
}

case class DynamicFieldParser(
  accessorPath: String, companion: DynamicValidationCompanion, schema: Schema) {
  val label = s"$accessorPath:${companion.validatorIdentifier}"

  val fieldAccessor: Any => Any = AvroObjMapper.getAvroFun(accessorPath, schema)

  def dynamicParser(avroRecord: GenericRecord): Any = {
    val fieldValue = fieldAccessor(avroRecord)
    companion.dynamicParser(fieldValue)
  }
}
