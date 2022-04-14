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
package com.spotify.elitzur.converters.avro.dynamic.dsl

import org.apache.avro.Schema

object AvroAccessorException {
  class InvalidDynamicFieldException(msg: String) extends Exception(msg)

  final val MISSING_TOKEN =
    "Leading '.' missing in the arg. Please prepend '.' to the arg"

  // TODO: Update docs on Dynamic and Magnolia based Elitzur and link it to exception below
  final val UNSUPPORTED_MAP_SCHEMA =
    "Map schema not supported. Please use Magnolia version of Elitzur."

  final val INVALID_UNION_SCHEMA =
    "Union schemas containing more than one non-null schemas is not supported."

  final val MISSING_ARRAY_TOKEN =
    """
      |Missing `[]` token for an array fields. All array fields should have `[]` token provided
      |in the input.
      |""".stripMargin

  final val NO_INTERNAL_SCHEMA =
    "Unable to extract the schema for the last field provided in the path."

  object InvalidDynamicFieldException {
    def apply(errs: Array[Throwable], schema: Schema): InvalidDynamicFieldException = {
      val concatErrMsg: String = errs.map(err => "\t".concat(err.getMessage)).mkString(",\n")
      new InvalidDynamicFieldException(
        s"Invalid field(s) for schema ${schema.toString}: \n$concatErrMsg")
    }

    // TODO: String is general to the apply method. Wrap accessorPath in the future.
    def apply(err: Throwable, accessorPath: String): InvalidDynamicFieldException = {
      new InvalidDynamicFieldException(s"Invalid field $accessorPath: ${err.getMessage}")
    }
  }
}
