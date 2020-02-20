package com.spotify.elitzur.validators

import scala.language.experimental.macros
import scala.reflect.macros._

@SuppressWarnings(Array("org.wartremover.warts.StringPlusAny"))
private[elitzur] object ValidatorMacros {

  private[this] val ShowWarnDefault = true
  private[this] val ShowWarnSettingRegex = "show-validator-fallback=(true|false)".r

  // Add a level of indirection to prevent the macro from capturing
  // $outer which would make the Coder serialization fail
  def wrappedValidator[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
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

          case q"com.spotify.elitzur.Validator.dispatch(new magnolia.SealedTrait($name, $subtypes, $annotations))" =>
            q"_root_.com.spotify.elitzur.Validator.dispatch(new magnolia.SealedTrait($name, $subtypes, Array()))"

          case q"""magnolia.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, $annotations)""" =>
            q"""_root_.magnolia.Magnolia.param[$tc, $t, $p]($name, $idx, $repeated, $tcParam, $defaultVal, Array())"""

          case t =>
            super.transform(tree)
        }
      }
    }
    // scalastyle:on line.size.limit
    val validator = removeAnnotations.transform(getLazyVal)

    validator
  }
  // scalastyle:on method.length

  //scalastyle:off line.size.limit
  /**
   * Makes it possible to configure fallback warnings by passing
   * "-Xmacro-settings:show-validator-fallback=true" as a Scalac option.
   * Stolen from scio here:
   * https://github.com/spotify/scio/blob/9379a2b8a6a6b30963841700f99ca2cf04857172/scio-macros/src/main/scala/com/spotify/scio/coders/CoderMacros.scala
   */
  //scalastyle:on line.size.limit
  private[this] def showWarn(c: whitebox.Context) =
    c.settings
      .collectFirst {
        case ShowWarnSettingRegex(value) =>
          value.toBoolean
      }
      .getOrElse(ShowWarnDefault)


  def issueFallbackWarning[T: c.WeakTypeTag](c: whitebox.Context): c.Tree = {
    import c.universe._

    val wtt = weakTypeOf[T]
    val TypeRef(_, sym, args) = wtt
    val typeName = sym.name
    val params = args.headOption
      .map { _ =>
        args.mkString("[", ",", "]")
      }
      .getOrElse("")
    val fullType = typeName + params

    val warning =
      s"""
         | Warning: No implicit Validator found for the following type:
         |
         |   >>  $wtt
         |
         | You can add a Validator for this type like this:
         |
         |    implicit val <yourValidatorName> = new IgnoreValidator[$fullType]
         |
         | If this is a primitive or a type other people use please consider contributing this
         | back to elitzur
         | """.stripMargin

    val shouldWarn = showWarn(c)
    // TODO this doesn't show up when using c.warning.  We might want to use that
    if (shouldWarn) c.echo(c.enclosingPosition, warning)

    val fallback = q"""new _root_.com.spotify.elitzur.validators.IgnoreValidator[$wtt]"""
    fallback
  }
}
