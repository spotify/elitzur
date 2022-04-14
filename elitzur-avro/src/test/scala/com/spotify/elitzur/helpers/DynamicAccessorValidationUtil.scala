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

package com.spotify.elitzur.helpers

import com.spotify.elitzur.MetricsReporter
import com.spotify.elitzur.converters.avro.dynamic.{
  DynamicAccessorValidator,
  DynamicFieldParser,
  RecordValidatorProperty
}
import com.spotify.elitzur.validators._
import org.apache.avro.Schema

object DynamicAccessorValidatorTestUtils {
  class TestMetricsReporter extends MetricsReporter {
    val map: scala.collection.mutable.Map[String, Int] =
      scala.collection.mutable.Map[String, Int]().withDefaultValue(0)
    override def reportValid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.valid") += 1
    override def reportInvalid(className: String, fieldName: String, validationType: String): Unit =
      map(s"$className.$fieldName.$validationType.invalid") += 1
    override def toString: String = map.toString()
    def getValid(className: String, fieldName: String, validationType: String): Int =
      map(s"$className.$fieldName.$validationType.valid")
    def getInvalid(className: String, fieldName: String, validationType: String): Int =
      map(s"$className.$fieldName.$validationType.invalid")
    def cleanSlate(): Unit = map.clear()
  }

  def metricsReporter(): MetricsReporter = new TestMetricsReporter
}

class DynamicAccessorValidationHelpers(
  input: Array[RecordValidatorProperty], schema: Schema
)(implicit metricsReporter: MetricsReporter){
  // The following class generates the accessor function based on the user provided input and the
  // Avro schema. It also uses the companion object provided in the input to determine how to
  // validate a given field.
  val dynamicRecordValidator = new DynamicAccessorValidator(input, schema)(metricsReporter)

  def getFieldParser(input: String, c: BaseCompanion[_, _]): DynamicFieldParser = {
    dynamicRecordValidator.fieldParsers
      .find(x => x.fieldLabel == s"$input:${c.validationType}").get
  }

  def getValidAndInvalidCounts(input: String, c: BaseCompanion[_, _]): (Int, Int) = {
    val parser = getFieldParser(input, c)
    val m = metricsReporter.asInstanceOf[DynamicAccessorValidatorTestUtils.TestMetricsReporter]
    val args = (
      dynamicRecordValidator.className,
      parser.fieldLabel,
      s"${parser.fieldLabel.split(":")(1)}Testing"
    )
    ((m.getValid _).tupled(args), (m.getInvalid _).tupled(args))
  }
}

