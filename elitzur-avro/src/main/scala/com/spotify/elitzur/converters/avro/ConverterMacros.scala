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
package com.spotify.elitzur.converters.avro

import scala.reflect.macros.whitebox

object ConverterMacros {
  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedRecordConverter[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._

    val magTree = magnolia1.Magnolia.gen[T](c)

    def getLazyVal =
      magTree match {
        case q"lazy val $_ = $body; $_" =>
          body

        case q"val $_ = $body; $_" =>
          body
      }

    // Remove annotations from magnolia since they are
    // not serializable and we don't use them anyway
    // scalastyle:off line.size.limit
    val removeAnnotations =
    new Transformer {
      override def transform(tree: Tree): c.universe.Tree = {
        tree match {
          case Apply(AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps),
          List(typeName, isObject, isValueClass, params, _)) =>
            Apply(AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps),
              List(typeName, isObject, isValueClass, params, q"""Array()"""))

          case q"""new magnolia1.CaseClass[$tc, $t]($typeName, $isObject, $isValueClass, $params, $_){ $body }""" =>
            q"""_root_.magnolia1.CaseClass[$tc, $t]($typeName, $isObject, $isValueClass, $params, Array()){ $body }"""

          case q"com.spotify.elitzur.AvroConverter.split(new magnolia1.SealedTrait($name, $subtypes, $_))" =>
            q"_root_.com.spotify.elitzur.AvroConverter.split(new magnolia1.SealedTrait($name, $subtypes, Array()))"

          case q"""magnolia1.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, $_)""" =>
            q"""_root_.magnolia1.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, Array())"""

          case _ =>
            super.transform(tree)
        }
      }
    }
    // scalastyle:on line.size.limit
    val transformer = removeAnnotations.transform(getLazyVal)

    transformer
  }
  // scalastyle:on method.length

}
