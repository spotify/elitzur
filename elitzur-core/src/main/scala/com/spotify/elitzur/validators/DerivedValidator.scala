package com.spotify.elitzur.validators

import java.lang.{StringBuilder => JStringBuilder}

import com.spotify.elitzur.{DataInvalidException, MetricsReporter}
import magnolia._

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final private[elitzur] case class DerivedValidator[T] private(caseClass: CaseClass[Validator, T])
                                                             (implicit reporter: MetricsReporter)
  extends Validator[T] {
  //scalastyle:off method.length cyclomatic.complexity
  override def validateRecord(a: PreValidation[T],
                              path: String = "",
                              outermostClassName: Option[String] = None,
                              config: ValidationRecordConfig = DefaultRecordConfig)
  : PostValidation[T] = {
    val ps = caseClass.parameters
    val cs = new Array[Any](ps.length)
    var i = 0
    var atLeastOneValid = false
    var atLeastOneInvalid = false

    val counterClassName =
      if (outermostClassName.isEmpty) caseClass.typeName.full else outermostClassName.get
    while (i < ps.length) {
      val p = ps(i)
      val deref = p.dereference(a.forceGet)
      val v =
        if (!p.typeclass.isInstanceOf[IgnoreValidator[_]]) {
          val name = new JStringBuilder(path.length + p.label.length)
            .append(path).append(p.label).toString
          //TODO: This get wrapped in unvalidated seems unnecessary, how can we get rid of it
          val o = if (
             p.typeclass.isInstanceOf[FieldValidator[_]]
          ) {
            val v = p.typeclass.validateRecord(Unvalidated(deref))
            val validationType = p.typeclass.asInstanceOf[FieldValidator[_]].validationType

            val c: ValidationFieldConfig = config.m
              .getOrElse(name, DefaultFieldConfig).asInstanceOf[ValidationFieldConfig]
            if (v.isValid) {
              if (c != NoCounter) {
                reporter.reportValid(
                  counterClassName,
                  name,
                  validationType)
              }
            }
            else if (v.isInvalid) {
              if (c == ThrowException) {
                throw new DataInvalidException(
                  s"Invalid value ${v.forceGet} found for field $path${p.label}")
              }
              if (c != NoCounter) {
                reporter.reportInvalid(
                  counterClassName,
                  name,
                  validationType
                )
              }
            }
            v
          } else {
            p.typeclass.validateRecord(
              Unvalidated(deref),
              new JStringBuilder(path.length + p.label.length + 1)
                .append(path).append(p.label).append(".").toString,
              Some(counterClassName),
              config
            )
          }

          if (o.isValid) {
            atLeastOneValid = true
          }
          else if (o.isInvalid) {
            atLeastOneInvalid = true
          }

          o.forceGet
        }
        else {
          deref
        }
      cs.update(i, v)
      i = i + 1
    }

    val record = caseClass.rawConstruct(cs)

    if (atLeastOneInvalid){
      Invalid(record)
    }
    else if (atLeastOneValid) {
      Valid(record)
    }
    else {
      IgnoreValidation(record)
    }
  }

  override def shouldValidate: Boolean = {
    val ps = caseClass.parameters
    var i = 0
    var shouldValidate = false

    while (i < ps.length) {
      val p = ps(i)
      if (p.typeclass.shouldValidate) {
        shouldValidate = true
      }
      i = i + 1
    }
    shouldValidate
  }
  //scalastyle:on method.length cyclomatic.complexity
}
