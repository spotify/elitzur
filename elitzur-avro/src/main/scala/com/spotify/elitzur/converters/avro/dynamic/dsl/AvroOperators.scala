///*
// * Copyright 2021 Spotify AB.
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing,
// * software distributed under the License is distributed on an
// * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// * KIND, either express or implied.  See the License for the
// * specific language governing permissions and limitations
// * under the License.
// */
//
//package com.spotify.elitzur.converters.avro.dynamic.dsl
//
//import org.apache.avro.Schema
//import java.{util => ju}
//
//trait BaseOperator {
//  val token: String
//  val avroSchemaEnums: ju.EnumSet[Schema.Type]
//  def apply(op: BaseFilter): BaseFilter
//
//  def validateSchema(schema: Schema.Type): Unit = {
//    if (!avroSchemaEnums.contains(schema)) { throw new Exception("abc") }
//  }
//}
//
//object NullableOperator extends BaseOperator {
//  override val token: String = "?"
//  override val avroSchemaEnums: ju.EnumSet[Schema.Type] = ju.EnumSet.allOf(classOf[Schema.Type])
//  def apply(op: BaseFilter): BaseFilter = NullableFilter(op)
//
//  //        val recursiveResult = AvroObjMapper.getAvroOperators(rest, normSchema)
//  //        val innerOps = AvroObjMapper.combineFns(recursiveResult.map(_.ops))
//  //        val filter = NullableFilterV2(innerOps)
//
//}
//
//object IterableOperator extends BaseOperator {
//  override val token: String = "[]"
//  override val avroSchemaEnums: ju.EnumSet[Schema.Type] = ju.EnumSet.of(Schema.Type.ARRAY)
//  def apply(op: BaseFilter): BaseFilter = NullableFilter(op)
//}
//
//object NoopOperator extends BaseOperator {
//  override val token: String = ""
//  override val avroSchemaEnums: ju.EnumSet[Schema.Type] = ju.EnumSet.allOf(classOf[Schema.Type])
//  def apply(op: BaseFilter): BaseFilter = NoopFilter()
//}
//
//object AvroOpUtil {
//  // revisit whether this will break if the ordering if flipped
//  // need schema to match
//  def mapTokenToOps(opToken: Option[String]): BaseOperator = {
//    opToken match {
//      case Some(NullableOperator.token) => NullableOperator
//      case Some(IterableOperator.token) => IterableOperator
//      //      case Some(IterableOperator.token + NullableOperator.token) => Seq(NullableOperator,
//      //        IterableOperator)
//      //      case Some(NullableOperator.token + IterableOperator.token) => Seq(NullableOperator,
//      //        IterableOperator)
//      case None => NoopOperator
//      case _ => throw new Exception("abcsd")
//    }
//  }
//}