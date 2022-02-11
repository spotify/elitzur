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

package com.spotify.elitzur.converters.avro.qaas

import com.google.cloud.bigquery.storage.v1beta1.AvroProto.AvroSchema
import com.spotify.elitzur.converters.avro.qaas.AvroFieldExtractorExceptions.InvalidAvroFieldOperationException
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

import java.util
import scala.annotation.tailrec
import scala.collection.convert.Wrappers
import scala.util.matching.Regex

// TODO: please rename this class
//object AvroFieldExtractorV2 {
//  private val hasNextLeaf: Regex = """^([a-zA-Z0-9]*)\.(.*)""".r
//  private val isLeafNode: Regex = """^([a-zA-Z0-9]*)$""".r
//  private val hasNextLeafArray: Regex = """^([a-zA-Z0-9]*)\[\]\.(.*)""".r
//  private val isLeafNodeArray: Regex = """^([a-zA-Z0-9]*)\[\]$""".r
//
//  private val PRIMITIVES: Set[Schema.Type] =
//    Set(Schema.Type.STRING, Schema.Type.LONG, Schema.Type.DOUBLE, Schema.Type.BOOLEAN,
//      Schema.Type.BYTES, Schema.Type.FLOAT, Schema.Type.INT, Schema.Type.NULL)
//
//  def someFun(path: String, avroSchema: AvroSchema): Unit = {
//    path match {
//      case hasNextLeaf(field, rest) =>
//
//    }
//  }
//
//
//}
