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
import java.{util => ju}
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import scala.jdk.CollectionConverters.collectionAsScalaIterableConverter
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

  private val fieldAccessor = AvroObjMapper.getAvroFunWithSchema(
    validatorProperty.accessorPath, schema)

  private val parsingFun: Any => Any = { fieldValue: Any =>
    val isAvroString = DynamicFieldParserUtil.isAvroString(schema)

    val fn = { x: Any => if (fieldAccessor.isNullable) {
        if (isAvroString) {
          Option(x).map(s => validatorProperty.companion.parseUnsafe(s.toString))
        } else {
          Option(x).map(validatorProperty.companion.parseUnsafe)
        }
      } else {
        if (isAvroString) {
          validatorProperty.companion.parseUnsafe(x.toString)
        } else {
          validatorProperty.companion.parseUnsafe(x)
        }
      }
    }

    if (fieldAccessor.isArray) {
      fieldValue.asInstanceOf[ju.List].asScala.toSeq.map(fn)
    } else {
      fn
    }

  }

  def fieldParser(avroRecord: GenericRecord): Any = {
    val fieldValue = fieldAccessor.accessorFn(avroRecord)
    parsingFun(fieldValue)
  }
}

object DynamicFieldParserUtil {

  def isAvroString(schema: Schema): Boolean = {
    if (schema.getType == Schema.Type.STRING) { true }
    else if (schema.getType == Schema.Type.UNION) { schema.getTypes.contains(Schema.Type.STRING) }
    else { false }
  }

}

case class RecordValidatorProperty(
  accessorPath: String, companion: BaseCompanion[_, _], validator: Validator[Any])
