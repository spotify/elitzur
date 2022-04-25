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
import com.spotify.elitzur.converters.avro.dynamic.dsl.BaseAccessor
import com.spotify.elitzur.converters.avro.dynamic.dsl.Implicits._
import com.spotify.elitzur.validators.{DynamicRecordValidator, Unvalidated, Validator}
import org.apache.avro.generic.GenericRecord

//scalastyle:off line.size.limit
class DynamicAccessorValidator(fieldParsers: Array[DynamicFieldParser])(implicit metricsReporter: MetricsReporter) extends Serializable {
//scalastyle:on line.size.limit
  final val className: String = this.getClass.getName

  val validator: DynamicRecordValidator = DynamicRecordValidator(
    fieldParsers.map(_.fieldValidator),
    fieldParsers.map(_.fieldLabel)
  )

  def validateRecord(avroRecord: GenericRecord): Unit = {
    val parseAllResult: Seq[Any] = fieldParsers.map(_.fieldParser(avroRecord))
    validator.validateRecord(Unvalidated(parseAllResult), outermostClassName = Some(className))
  }
}

class DynamicFieldParser(
  accessorInput: String,
  accessorCompanion: DynamicAccessorCompanion[_, _],
  accessorOps: List[BaseAccessor]
)(implicit metricsReporter: MetricsReporter) extends Serializable {
  private val validatorOp = accessorOps.toValidatorOp
  private val fieldFn: Any => Any = accessorCompanion.getPreprocessorForValidator(validatorOp)

  private[dynamic] val fieldValidator: Validator[Any] = accessorCompanion.getValidator(validatorOp)
  private[dynamic] val fieldLabel: String = accessorInput
  private[dynamic] def fieldParser(avroRecord: GenericRecord): Any = {
    val fieldValue = accessorOps.combineFns(avroRecord)
    fieldFn(fieldValue)
  }
}
