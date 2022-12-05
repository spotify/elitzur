package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import SchemaToAccessorOpsExceptionMsg.{InvalidDynamicFieldException, MISSING_ARRAY_TOKEN}
import com.spotify.elitzur.converters.avro.dynamic.schema.{SchemaType, SchemaTypeUtil}

abstract class BaseAccessorLogic[T: SchemaType](schema: T, fieldTokens: FieldTokens) {
  val accessor: BaseAccessor
  val accessorWithMetadata: SchemaToAccessorOpsTracker[T]
  def fieldAccessorFn: Any => Any = SchemaTypeUtil.getFieldFromSchema(schema, fieldTokens.field)
}

class IndexAccessorLogic[T: SchemaType](schema: T, fieldTokens: FieldTokens)
  extends BaseAccessorLogic[T](schema, fieldTokens) {
  override val accessor: BaseAccessor = IndexAccessor(fieldAccessorFn)
  override val accessorWithMetadata: SchemaToAccessorOpsTracker[T] =
    SchemaToAccessorOpsTracker(accessor, schema, fieldTokens.rest)
}

class NullableAccessorLogic[T: SchemaType](
  schema: T,
  fieldTokens: FieldTokens,
  innerAccessors: List[BaseAccessor]
) extends BaseAccessorLogic[T](schema, fieldTokens) {
  override val accessor: BaseAccessor = {
    if (innerAccessors.isEmpty) {
      IndexAccessor(fieldAccessorFn)
    } else {
      NullableAccessor(fieldAccessorFn, innerAccessors)
    }
  }
  override val accessorWithMetadata: SchemaToAccessorOpsTracker[T] =
    SchemaToAccessorOpsTracker(accessor, schema, None)
}

class ArrayAccessorLogic[T: SchemaType](
  schema: T,
  fieldTokens: FieldTokens,
  innerAccessors: List[BaseAccessor]
) extends BaseAccessorLogic[T](schema, fieldTokens) {
  private final val arrayToken = "[]"

  override val accessor: BaseAccessor = {
    if (fieldTokens.rest.isEmpty) {
      ArrayNoopAccessor(fieldAccessorFn, innerAccessors, fieldTokens.op.contains(arrayToken))
    } else {
      if (!fieldTokens.op.contains(arrayToken)) {
        throw new InvalidDynamicFieldException(MISSING_ARRAY_TOKEN)
      }
      if (getFlattenFlag(innerAccessors)) {
        ArrayFlatmapAccessor(fieldAccessorFn, innerAccessors)
      } else {
        ArrayMapAccessor(fieldAccessorFn, innerAccessors)
      }
    }
  }
  override val accessorWithMetadata: SchemaToAccessorOpsTracker[T] =
    SchemaToAccessorOpsTracker(accessor, schema, None)

  private def getFlattenFlag(ops: List[BaseAccessor]): Boolean = {
    ops.foldLeft(false)((accBoolean, currAccessor) => {
      val hasArrayAccessor = currAccessor match {
        case n: NullableAccessor => getFlattenFlag(n.innerOps)
        case a: ArrayNoopAccessor => a.flatten
        case _: ArrayMapAccessor | _: ArrayFlatmapAccessor => true
        case _ => false
      }
      accBoolean || hasArrayAccessor
    })
  }
}
