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
package com.spotify.elitzur.converters.avro.dynamic.validator.core

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.FieldAccessor
import com.spotify.elitzur.converters.avro.dynamic.schema.SchemaType
import com.spotify.elitzur.validators.{DynamicRecordValidator, Unvalidated, Validator}
import org.slf4j.LoggerFactory

import scala.reflect.runtime.universe.TypeTag

class DynamicAccessorValidator[R: SchemaType: TypeTag, T: RecordType: TypeTag](
  fieldParsers: Array[DynamicFieldParser[R, T]]
)(implicit metricsReporter: MetricsReporter) extends Serializable {
  final val className: String = this.getClass.getName

  val validator: DynamicRecordValidator = DynamicRecordValidator(
    fieldParsers.map(_.fieldValidator),
    fieldParsers.map(_.fieldLabel)
  )

  def validateRecord(record: T): Unit = {
    val parseAllResult: Seq[Any] = fieldParsers.map(_.fieldParser(record))
    validator.validateRecord(Unvalidated(parseAllResult), outermostClassName = Some(className))
  }
}

class DynamicFieldParser[R: SchemaType: TypeTag, T: RecordType: TypeTag](
  accessorInput: String,
  accessorCompanion: DynamicAccessorCompanion[_, _],
  accessorOps: FieldAccessor[R]
)(implicit metricsReporter: MetricsReporter) extends Serializable {
  private val logger = LoggerFactory.getLogger(this.getClass)

  // TODO: better string splitting
  private val fieldAccessorOp = accessorOps.getFieldAccessor(
    accessorInput.split(":").headOption.get)

  private val validatorOp = fieldAccessorOp.validationOp
  private val prepForFieldValidatorFn: Any => Any =
    accessorCompanion.typedFieldValueFn[R](validatorOp)

  private[dynamic] val fieldValidator: Validator[Any] = accessorCompanion.getValidator(validatorOp)
  private[dynamic] val fieldLabel: String = accessorInput
  private[dynamic] def fieldParser(record: T): Any = {
    val fieldValue = fieldAccessorOp.accessorFns(record)
    prepForFieldValidatorFn(fieldValue)
  }

  logger.info(
    s"""
       |The field validator input of '$accessorInput' resulted in:
       |\tAccessors: ${fieldAccessorOp.accessors.toString}
       |\tValidators: ${validatorOp.map(_.getClass.getSimpleName).toString}
       |""".stripMargin
  )
}
