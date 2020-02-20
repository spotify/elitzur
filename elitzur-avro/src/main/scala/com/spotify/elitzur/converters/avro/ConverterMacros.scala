package com.spotify.elitzur.converters.avro

import scala.reflect.macros.whitebox

object ConverterMacros {
  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedRecordConverter[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._

    val magTree = magnolia.Magnolia.gen[T](c)

    def getLazyVal =
      magTree match {
        case q"lazy val $name = $body; $rest" =>
          body

        case q"val $name = $body; $rest" =>
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
          List(typeName, isObject, isValueClass, params, annotations)) =>
            Apply(AppliedTypeTree(Select(pack, TypeName("CaseClass")), ps),
              List(typeName, isObject, isValueClass, params, q"""Array()"""))

          case q"""new magnolia.CaseClass[$tc, $t]($typeName, $isObject, $isValueClass, $params, $annotations){ $body }""" =>
            q"""_root_.magnolia.CaseClass[$tc, $t]($typeName, $isObject, $isValueClass, $params, Array()){ $body }"""

          case q"com.spotify.elitzur.AvroConverter.dispatch(new magnolia.SealedTrait($name, $subtypes, $annotations))" =>
            q"_root_.com.spotify.elitzur.AvroConverter.dispatch(new magnolia.SealedTrait($name, $subtypes, Array()))"

          case q"""magnolia.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, $annotations)""" =>
            q"""_root_.magnolia.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, Array())"""

          case t =>
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
