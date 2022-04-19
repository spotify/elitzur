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

//scalastyle:off line.size.limit
class DynamicAccessorValidator(fieldParsers: Array[DynamicFieldParser])(implicit metricsReporter: MetricsReporter) {
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
  fieldPathStr: String,
  validationTypeStr: String,
  accessorCompanion: DynamicAccessorCompanion[_, _],
  schema: Schema
) {
  private val validatorModifier: Modifier = accessorCompanion.getModifier(validationTypeStr)
  private val fieldAccessor = AvroObjMapper.getAvroFun(fieldPathStr, schema)

  private[dynamic] val fieldLabel: String = s"$fieldPathStr:${accessorCompanion.validationType}"
  private[dynamic] val fieldValidator: Validator[Any] =
    accessorCompanion.getValidator(validatorModifier)
  private[dynamic] val fieldFn: Any => Any =
    accessorCompanion.getPreprocessorForValidator(validatorModifier)

  private[dynamic] def fieldParser(avroRecord: GenericRecord): Any = {
    val fieldValue = fieldAccessor(avroRecord)
    fieldFn(fieldValue)
  }
}
