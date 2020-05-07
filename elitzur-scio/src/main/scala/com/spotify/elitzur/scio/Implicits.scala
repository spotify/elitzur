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
package com.spotify.elitzur.scio

import com.spotify.elitzur.MetricsReporter
import ValidatorDoFns.{ValidatorDoFn, ValidatorDoFnWithResult}
import com.spotify.elitzur.converters.avro.AvroConverter
import com.spotify.elitzur.validators.{PostValidation, ValidationRecordConfig, Validator}
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import org.apache.avro.generic.GenericRecord
import org.apache.beam.sdk.transforms.ParDo

import scala.reflect.ClassTag

trait Implicits {
  implicit val metricsReporter: MetricsReporter = new ScioMetricsReporter

  implicit class SCollectionImplicitValidatorFns[T: Coder](sc: SCollection[T])(
    implicit vr: Validator[T]) {

    def validate(conf: ValidationRecordConfig = ValidationRecordConfig())
    : SCollection[T] = {
      sc.withName("validate").applyTransform(ParDo.of(new ValidatorDoFn(vr, config = conf)))
    }

    def validateWithResult(conf: ValidationRecordConfig = ValidationRecordConfig())
    : SCollection[PostValidation[T]] = {
      sc.withName("validateWithResult")
        .applyTransform(ParDo.of(new ValidatorDoFnWithResult[T](vr, conf)))
    }
  }

  implicit class SCollFromAvroConverter[GR <: GenericRecord : Coder](sc: SCollection[GR]) {
    def fromAvro[T: Coder](implicit c: AvroConverter[T]): SCollection[T] = {
      sc.withName("fromAvro").applyTransform(ParDo.of(new FromAvroConverterDoFn(c)))
    }
  }

  implicit class SCollToAvroConverter[T: AvroConverter: Coder](sc: SCollection[T]) {
    def toAvro[GR <: GenericRecord: Coder : ClassTag](implicit c: AvroConverter[T])
    : SCollection[GR] = {
      sc.withName("toAvro").applyTransform(ParDo.of(new ToAvroConverterDoFn(c)))
    }
  }

  implicit class SCollToAvroDefaultConverter[T: AvroConverter: Coder](sc: SCollection[T]) {
    def toAvroDefault[GR <: GenericRecord: Coder : ClassTag](defaultR: GR)
                                                            (implicit c: AvroConverter[T])
    : SCollection[GR] = {
      sc.withName("toAvroDefault").applyTransform(
        ParDo.of(new ToAvroDefaultConverterDoFn(defaultR, c)))
    }
  }
}
