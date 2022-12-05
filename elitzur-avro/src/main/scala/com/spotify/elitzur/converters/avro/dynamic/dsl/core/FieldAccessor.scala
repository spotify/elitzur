package com.spotify.elitzur.converters.avro.dynamic.dsl.core

import com.spotify.elitzur.converters.avro.dynamic.dsl.avro.AvroSchemaToAccessorOps
import com.spotify.elitzur.converters.avro.dynamic.dsl.bq.BqSchemaToAccessorOps
import com.spotify.elitzur.converters.avro.dynamic.schema.{BqSchema, SchemaType}
import com.spotify.elitzur.converters.avro.dynamic.validator.core.{ValidatorOp, ValidatorOpsUtil}
import org.apache.avro.Schema

import scala.collection.mutable

class FieldAccessor[T: SchemaType](schema: T) extends Serializable {
  private val mapToAccessor: mutable.Map[String, FieldAccessorConfig] =
    mutable.Map.empty[String, FieldAccessorConfig]

  private def getFieldAccessors(fieldPath: String): List[BaseAccessor] = schema match {
    case avro: Schema => AvroSchemaToAccessorOps.getFieldAccessorOps(fieldPath, avro).map(_.ops)
    case bq: BqSchema => BqSchemaToAccessorOps.getFieldAccessorOps(fieldPath, bq).map(_.ops)
  }

  def getFieldAccessor(fieldPath: String): FieldAccessorConfig = {
    if (!mapToAccessor.contains(fieldPath)) {
      val accessorOps = getFieldAccessors(fieldPath)
      mapToAccessor += (
        fieldPath -> FieldAccessorConfig(
          accessorOps,
          AccessorOpsUtil.combineFns(accessorOps),
          ValidatorOpsUtil.toValidatorOp(accessorOps))
      )
    }
    mapToAccessor(fieldPath)
  }
}

case class FieldAccessorConfig(
  accessors: List[BaseAccessor],
  accessorFns: Any => Any,
  validationOp: List[ValidatorOp]
) extends Serializable
