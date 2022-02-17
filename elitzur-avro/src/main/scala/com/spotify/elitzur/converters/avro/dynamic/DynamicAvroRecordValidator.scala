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
import org.apache.avro.generic.GenericRecord

import scala.util.matching.Regex

case class QaasAvroFieldValidator(
  avroPathStr: String, qaasValidationCompanion: QaasValidationCompanion) {

  val label: String = s"$avroPathStr:${qaasValidationCompanion.validatorIdentifier}"

  def getValidationClassWithData(avroRecord: GenericRecord): Any = {
    val fn = AvroObjMapper.getAvroFun(this.avroPathStr, avroRecord.getSchema)
    qaasValidationCompanion.validatorCheckParser(fn(avroRecord))
  }

}

class QaasAvroRecordValidator(
  validationArgs: Array[String],
  validatorCompanion: Map[String, QaasValidationCompanion]
)(implicit val metricsReporter: MetricsReporter) {

  final val className: String = "com.spotify.DynamicClass"

  // a more stringent input parsing / meaningful exception messages to be added below
  val validationInputs: Array[QaasAvroFieldValidator] = {
    val validationArgRegex: Regex = """^(.*):(.*)$""".r
    validationArgs.flatMap {
      case validationArgRegex(fieldPath, validatorName) =>
        Some(QaasAvroFieldValidator(fieldPath, validatorCompanion(validatorName.toUpperCase)))
      case _ => None
    }
  }

  val recordValidator: DynamicRecordValidator = DynamicRecordValidator(
    validationInputs.map(_.qaasValidationCompanion.validator).asInstanceOf[Array[Validator[Any]]],
    validationInputs.map(_.label)
  )

  def validateRecord(avroRecord: GenericRecord): Unit = {
    val seqValidationClassOnAvroRecord: Seq[Any] = validationInputs
      .map(_.getValidationClassWithData(avroRecord))
    recordValidator.validateRecord(
      Unvalidated(seqValidationClassOnAvroRecord),
      outermostClassName = Some(className)
    )
  }
}
