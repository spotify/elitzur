package com.spotify.elitzur.scio

import java.lang.reflect.{ParameterizedType, Type}
import java.lang.{StringBuilder => JStringBuilder}

import com.spotify.elitzur.CounterTypes
import com.spotify.elitzur.validators.ValidationStatus
import com.spotify.scio.{ScioMetrics, ScioResult, metrics}
import org.apache.beam.sdk.metrics.{Counter, MetricName}

import scala.annotation.tailrec

object ElitzurMetrics {

  /** construct Beam counter from parts of counter name */
  private[elitzur] def getCounter(className: String,
                                  fieldName: String,
                                  validationType: String,
                                  state: CounterTypes.Value): Counter = {
    val stateStr = state.toString
    val sb =
      new JStringBuilder(fieldName.length + 1 + validationType.length + 8 + stateStr.length)
    // This method is called very frequently (per-elitzur field per record) and building strings
    // the scala way is slower than expected (it seems to create multiple string builders)
    val counterName =
    sb
      .append(fieldName)
      .append("/")
      .append(validationType)
      .append("/Elitzur")
      .append(stateStr)
      .toString
    ScioMetrics.counter(className, counterName)
  }

  private[elitzur] def getClassNameFromCounterName(counterName: String): String =
    counterName.split("/")(0)

  private[elitzur] def getFieldNameFromCounterName(counterName: String): String =
    counterName.split("/")(1)

  private[elitzur] def getValidationTypeFromCounterName(counterName: String): String =
    counterName.split("/")(2)

  private[elitzur] def getCounterTypeFromCounterName(counterName: String): String =
    counterName.split("/")(3)

  private[elitzur] def getValidationTypeFromCaseClass(className: Class[_], fieldName: String)
  : String =
    getValidationTypeFromCaseClass(className, fieldName.split("\\.")).getSimpleName

  private def getParameterizedInnerType(genericType: Type): Type = {
    // removes one layer of type nesting from reflection
    // workaround found via https://stackoverflow.com/a/11165045
    genericType
      .asInstanceOf[ParameterizedType]
      .getActualTypeArguments()(0)
  }

  private def unwrapOptionType(optType: Type): Class[_] =
    getParameterizedInnerType(optType).asInstanceOf[Class[_]]

  private def unwrapValidationStatus(vsType: Type): Class[_] = {
    // assume we either have a ValidationStatus[Option[T]] or a ValidationStatus[T]
    val innerType = getParameterizedInnerType(vsType)
    innerType match {
      case it: Class[_] =>
        // if innerType is a Class[_] then the nested type wasn't itself parameterized
        it
      case pt: ParameterizedType =>
        // we can't cast this to a Class, so it's an Option[T], remove one layer and cast that
        getParameterizedInnerType(pt).asInstanceOf[Class[_]]
    }
  }

  // scalastyle:off cyclomatic.complexity
  @tailrec
  private def getValidationTypeFromCaseClass(caseClassClass: Class[_],
                                             fieldNames: Seq[String]): Class[_] = {

    val firstFieldName = fieldNames(0)
    val firstFieldClass: Class[_] = caseClassClass.getDeclaredField(firstFieldName).getType
    val firstFieldGenericType: Type = caseClassClass.getDeclaredField(firstFieldName).getGenericType

    val isOption = classOf[Option[_]].equals(firstFieldClass)
    val isWrapped = classOf[ValidationStatus[_]].isAssignableFrom(firstFieldClass)

    fieldNames match {
      case Seq(_) if isWrapped =>
        unwrapValidationStatus(firstFieldGenericType)
      case Seq(_) if isOption =>
        // remove one layer of parameterization only
        getParameterizedInnerType(firstFieldGenericType).asInstanceOf[Class[_]]
      case Seq(_) =>
        // no parameterization
        firstFieldClass
      case Seq(_, tail@_*) if isOption =>
        getValidationTypeFromCaseClass(unwrapOptionType(firstFieldGenericType), tail)
      case Seq(_, tail@_*) if isWrapped =>
        getValidationTypeFromCaseClass(unwrapValidationStatus(firstFieldGenericType), tail)
      case Seq(_, tail@_*) =>
        getValidationTypeFromCaseClass(firstFieldClass, tail)
    }
  }

  // scalastyle:on cyclomatic.complexity

  /** return subset of all Scio counters named with Elitzur */
  def getElitzurCounters(sr: ScioResult): Map[MetricName, metrics.MetricValue[Long]] = {
    sr.allCounters
      .filter(counter => counter._1.toString.contains("Elitzur"))
  }
}
