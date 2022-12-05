package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import com.spotify.elitzur.converters.avro.dynamic.dsl.core.SchemaToAccessorOpsExceptionMsg._
import com.spotify.elitzur.converters.avro.dynamic.schema.SchemaType

import scala.annotation.tailrec
import scala.util.{Failure, Success, Try}

abstract class SchemaToAccessorOps[T: SchemaType] {
  def isRequired(fieldSchema: T): Boolean

  def isRepeated(fieldSchema: T): Boolean

  def isNullable(fieldSchema: T): Boolean

  def isNotSupported(fieldSchema: T): Boolean

  def getFieldSchema(schema: T, fieldName: String): T

  def getElemFieldSchema(schema: T): T

  def getNonNullableFieldSchema(schema: T): T

  @tailrec
  private[elitzur] final def getFieldAccessorOps(
    fieldPath: String,
    parentSchema: T,
    accAccessorOps: List[SchemaToAccessorOpsTracker[T]] = List.empty[SchemaToAccessorOpsTracker[T]]
  ): List[SchemaToAccessorOpsTracker[T]] = {
    val currentAccessorOp = mapToAccessors(fieldPath, parentSchema)
    val appAccessorOps = accAccessorOps :+ currentAccessorOp
    currentAccessorOp.rest match {
      case Some(remainingPath) =>
        getFieldAccessorOps(remainingPath, currentAccessorOp.schema, appAccessorOps)
      case _ => appAccessorOps
    }
  }

  //scalastyle:off method.length cyclomatic.complexity
  def mapToAccessors(path: String, parentSchema: T): SchemaToAccessorOpsTracker[T] = {
    val fieldTokens = FieldTokens(path)
    val fieldSchema = Try(getFieldSchema(parentSchema, fieldTokens.field)) match {
      case Success(s) => s
      case Failure(_) =>
        throw new InvalidDynamicFieldException(s"$path not found in ${parentSchema.toString}")
    }

    def mapToAccessorsInternal(
      fieldSchema: T,
      fieldTokens: FieldTokens
    ): SchemaToAccessorOpsTracker[T] = {
      fieldSchema match {
        case _schema if isRequired(_schema) =>
          new IndexAccessorLogic(_schema, fieldTokens).accessorWithMetadata
        case _schema if isNullable(_schema) =>
          val nonNullSchema = getNonNullableFieldSchema(_schema)
          val currAccessor = mapToAccessorsInternal(nonNullSchema, fieldTokens)
          val remainingAccessors = getInnerAccessors(nonNullSchema, currAccessor.rest)
          new NullableAccessorLogic(
            nonNullSchema, fieldTokens, currAccessor.ops :: remainingAccessors).accessorWithMetadata
        case _schema if isRepeated(_schema) =>
          val elemSchema = getElemFieldSchema(_schema)
          val remainingAccessors = if (fieldTokens.rest.isEmpty && !isRequired(elemSchema)) {
            List(mapToAccessorsInternal(elemSchema, fieldTokens).ops)
          } else {
            getInnerAccessors(elemSchema, fieldTokens.rest)
          }
          new ArrayAccessorLogic(elemSchema, fieldTokens, remainingAccessors).accessorWithMetadata
        case _schema if isNotSupported(_schema) =>
          throw new Exception(s"Unsupported schema type: ${_schema.toString}")
      }
    }

    mapToAccessorsInternal(fieldSchema, fieldTokens)
  }
  //scalastyle:on method.length cyclomatic.complexity

  private def getInnerAccessors(schema: T, remainingPath: Option[String]): List[BaseAccessor] = {
    if (remainingPath.isDefined) {
      getFieldAccessorOps(remainingPath.get, schema).map(_.ops)
    } else {
      List.empty[BaseAccessor]
    }
  }
}

case class SchemaToAccessorOpsTracker[T: SchemaType](
  ops: BaseAccessor, schema: T, rest: Option[String])
