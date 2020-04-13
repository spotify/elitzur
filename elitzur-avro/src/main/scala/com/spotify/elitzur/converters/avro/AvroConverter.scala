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

//scalastyle:off line.size.limit
import java.util.Collections

import com.spotify.elitzur.validators.{BaseValidationType, DynamicCompanionImplicit, DynamicValidationType, SimpleCompanionImplicit, Unvalidated, ValidationStatus}
import org.apache.avro.generic._
import com.spotify.scio.coders.Coder

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.reflect.ClassTag
import com.spotify.elitzur.{Utils => SharedUtils}
import magnolia._
import org.apache.avro.Schema
import enumeratum._

import scala.collection.JavaConverters._
import scala.language.experimental.macros
import scala.language.{higherKinds, reflectiveCalls}
//scalastyle:on line.size.limit

trait AvroConverter[T] extends Serializable {
  def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): T
  def toAvro(v: T, schema: Schema): Any
  def toAvroDefault(v: T, defaultValueRecord: GenericContainer): Any
}

class PrimitiveConverter[T] extends AvroConverter[T] {
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): T = {
    // null.asInstanceOf[Long] == 0L in scala
    // because scala.Long == primitive java.lang.long (lowercase-l)
    // see the spec change to represent this behavior at
    // https://github.com/scala/scala-dist/commit/804c390adf62fd4380b29861cdcc3c35d6194093
    // found via https://github.com/scala/bug/issues/1245
    if (v == null) {
      throw new NullPointerException("Expected non-optional field to be non-null in Avro. " +
        "To fix, declare your Elitzur case class with any nullable Avro union fields " +
        "wrapped in options instead of declaring as the type directly.")
    } else {
      v.asInstanceOf[T]
    }
  }

  override def toAvro(v: T, schema: Schema): Any = v.asInstanceOf[Any]

  override def toAvroDefault(v: T, defaultGenericContainer: GenericContainer): Any =
    v.asInstanceOf[Any]
}

private[elitzur] class OptionConverter[T: AvroConverter] extends AvroConverter[Option[T]] {
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): Option[T] = {
    v match {
      case null => Option.empty[T]
      case _ => Option(implicitly[AvroConverter[T]].fromAvro(v, schema))
    }
  }
  override def toAvro(v: Option[T], schema: Schema): Any = {
    v match {
      case None => null
      case Some(t) => implicitly[AvroConverter[T]].toAvro(t, schema)
    }
  }

  override def toAvroDefault(v: Option[T], defaultGenericContainer: GenericContainer): Any  = {
    v match {
      case None => null
      case Some(t) => implicitly[AvroConverter[T]].toAvroDefault(t, defaultGenericContainer)
    }
  }
}

private[elitzur] class AvroWrappedValidationConverter[T: AvroConverter]
  extends AvroConverter[ValidationStatus[T]] {
  //TODO: This assumes validation has not been done. Can we verify this at compile time
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None)
  : ValidationStatus[T] = {
    Unvalidated(implicitly[AvroConverter[T]].fromAvro(v, schema))
  }

  //TODO: This assumes validation has already been done. Can we verify this at compile time
  override def toAvro(v: ValidationStatus[T], schema: Schema): Any = {
    implicitly[AvroConverter[T]].toAvro(v.forceGet, schema)
  }

  override def toAvroDefault(v: ValidationStatus[T], defaultGenericContainer: GenericContainer)
  : Any = {
    implicitly[AvroConverter[T]].toAvroDefault(v.forceGet, defaultGenericContainer)
  }
}

//scalastyle:off line.size.limit structural.type
private[elitzur] class AvroSimpleTypeConverter[LT <: BaseValidationType[T]: ({type L[x] = SimpleCompanionImplicit[T, x]})#L, T: AvroConverter]
  extends AvroConverter[LT] {
  //scalastyle:on line.size.limit structural.type

  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): LT = {
      val c = implicitly[SimpleCompanionImplicit[T, LT]].companion
      c.parse(implicitly[AvroConverter[T]].fromAvro(v, schema))
  }

  override def toAvro(v: LT, schema: Schema): Any = v.data

  override def toAvroDefault(v: LT, defaultGenericContainer: GenericContainer): Any = v.data
}

private[elitzur] class AvroOptionConverter[T <: BaseValidationType[_]: AvroConverter]
  extends AvroConverter[Option[T]] {
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): Option[T] = {
    v match {
      case null => Option.empty[T]
      case _ => Option(implicitly[AvroConverter[T]].fromAvro(v, schema, doc))
    }
  }
  override def toAvro(v: Option[T], schema: Schema): Any = {
    v match {
      case None => null
      case Some(t) => implicitly[AvroConverter[T]].toAvro(t, schema)
    }
  }

  override def toAvroDefault(v: Option[T], defaultGenericContainer: GenericContainer): Any = {
    v match {
      case None => null
      case Some(t) => implicitly[AvroConverter[T]].toAvroDefault(t, defaultGenericContainer)
    }
  }
}

private[elitzur] class AvroStatusConverter[T <: BaseValidationType[_]: AvroConverter]
  extends AvroConverter[ValidationStatus[T]] {
  //TODO: This assumes validation has not been done. Can we verify this at compile time
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): ValidationStatus[T] = {
    Unvalidated(implicitly[AvroConverter[T]].fromAvro(v, schema, doc))
  }

  //TODO: This assumes validation has already been done. Can we verify this at compile time
  override def toAvro(v: ValidationStatus[T], schema: Schema): Any = {
    implicitly[AvroConverter[T]].toAvro(v.forceGet, schema)
  }

  override def toAvroDefault(v: ValidationStatus[T], defaultGenericContainer: GenericContainer)
  : Any = {
    implicitly[AvroConverter[T]].toAvroDefault(v.forceGet, defaultGenericContainer)
  }
}

private[elitzur] class AvroStatusOptionConverter[T <: BaseValidationType[_]: AvroConverter]
  extends AvroConverter[ValidationStatus[Option[T]]] {
  //TODO: This assumes validation has not been done. Can we verify this at compile time
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None)
  : ValidationStatus[Option[T]] = {
    Unvalidated(v match {
      case null => Option.empty[T]
      case _ => Option(implicitly[AvroConverter[T]].fromAvro(v, schema, doc))
    })
  }

  override def toAvro(v: ValidationStatus[Option[T]], schema: Schema): Any = {
    v.forceGet match {
      case None => null
      case _ => implicitly[AvroConverter[T]].toAvro(v.forceGet.get, schema)
    }
  }

  override def toAvroDefault(v: ValidationStatus[Option[T]],
                             defaultGenericContainer: GenericContainer)
  : Any = {
    v.forceGet match {
      case None => null
      case _ => implicitly[AvroConverter[T]].toAvroDefault(v.forceGet.get, defaultGenericContainer)
    }
  }
}


/**
  * @tparam T The inner type the sequence contains
  * @tparam C The type of the sequence we want to convert
  */
private[elitzur] class AvroSeqConverter[T: AvroConverter: Coder: ClassTag, C[_]](
    builderFn: () => mutable.Builder[T, C[T]])(implicit toSeq: C[T] => TraversableOnce[T])
  extends AvroConverter[C[T]] {
  //TODO: Initialize lazily maybe?
  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): C[T] = {
    val c = implicitly[AvroConverter[T]]
    val builder: mutable.Builder[T, C[T]] = builderFn()
    v.asInstanceOf[java.lang.Iterable[Any]].asScala
      .foreach(elem => builder += c.fromAvro(elem, schema.getElementType, doc))
    builder.result
  }

  override def toAvro(v: C[T], schema: Schema): Any = {
    val c = implicitly[AvroConverter[T]]
    // avro expects a list
    val output: java.util.List[Any] = new java.util.ArrayList[Any]
    for (i <- v) {
      output.add(c.toAvro(i, schema.getElementType))
    }
    output
  }

  override def toAvroDefault(v: C[T], defaultGenericContainer: GenericContainer): Any = {
    val c = implicitly[AvroConverter[T]]
    val output: java.util.List[Any] = new java.util.ArrayList[Any]
    // We use the first record in the default list as the default - otherwise length might not match
    val firstDefault = defaultGenericContainer.asInstanceOf[GenericArray[_]].get(0)
    val isNestedRecord = AvroElitzurConversionUtils
      .isAvroRecordType(defaultGenericContainer.getSchema.getElementType)
    if (isNestedRecord) {
      for (i <- v) {
        output.add(c.toAvroDefault(i, firstDefault.asInstanceOf[GenericRecord]))
      }
    } else {
      for (i <- v) {
        output.add(c.toAvroDefault(i, defaultGenericContainer))
      }
    }
    output
  }
}

//scalastyle:off line.size.limit structural.type
// Use a type lambda so that we can get Companion as a context bound since magnolia doesn't play
//  nice with additional implicit parameters on the classlevel
private[elitzur] class AvroDynamicTypeConverter[LT <: DynamicValidationType[T, _, LT]: ({type L[x] = DynamicCompanionImplicit[T, _, x]})#L, T: AvroConverter]
  extends AvroConverter[LT] {
  //scalastyle:on line.size.limit structural.type

  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): LT = {
    val c = implicitly[DynamicCompanionImplicit[T, _, LT]].companion
    c.parse(implicitly[AvroConverter[T]].fromAvro(v, schema))
  }

  override def toAvro(v: LT, schema: Schema): Any = v.data

  override def toAvroDefault(v: LT, defaultGenericContainer: GenericContainer): Any = v.data
}

private[elitzur] class AvroEnumConverter[T <: enumeratum.EnumEntry: Enum] extends AvroConverter[T] {
  override def fromAvro(v: Any, schema: Schema, doc: Option[String]): T =
    implicitly[Enum[T]].withName(v.toString)

  override def toAvro(v: T, schema: Schema): Any = new GenericData.EnumSymbol(schema, v.entryName)

  override def toAvroDefault(v: T, defaultGenericContainer: GenericContainer): Any =
    v.asInstanceOf[Any]
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
final private[elitzur] case class DerivedConverter[T] private(caseClass: CaseClass[AvroConverter,T])
  extends AvroConverter[T] {

  override def fromAvro(v: Any, schema: Schema, doc: Option[String] = None): T = {
    val ps = caseClass.parameters
    val cs = new Array[Any](ps.length)
    var i = 0

    while (i < ps.length) {
      val p = ps(i)
      // Since our labels are camelCase in scala and snake_case in avro, we convert to snake to
      // retrieve the param ref
      val fieldName = if (schema.getField(p.label) == null) {
        SharedUtils.camelToSnake(p.label)
      } else {
        p.label
      }

      val field = schema.getField(fieldName)
      val innerSchema = field.schema()
      val doc = Option(field.doc())
      // We assume union types are used for optional fields only, can be adjusted in the future
      val schemaParam = innerSchema.getType match {
        case Schema.Type.UNION =>
          val candidateSchema = innerSchema.getTypes.asScala.find(_.getType != Schema.Type.NULL)
          candidateSchema.map(_.getType) match {
            case Some(Schema.Type.RECORD) => candidateSchema.get
            case _ => innerSchema
          }
        case _ => innerSchema
      }
      cs.update(i,
        p.typeclass.fromAvro(
          v.asInstanceOf[GenericRecord].get(fieldName),
          schemaParam,
          doc
        )
      )
      i += 1
    }
    caseClass.rawConstruct(cs)
  }

  override def toAvro(v: T, schema: Schema): Any = {
    val ps = caseClass.parameters
    var i = 0
    val cleanSchema = AvroElitzurConversionUtils.getNestedRecordSchema(schema)
    val builder = new GenericRecordBuilder(cleanSchema)

    while (i < ps.length) {
      val p = ps(i)
      val deref = p.dereference(v)

      val fieldName = if (cleanSchema.getField(p.label) == null) {
        SharedUtils.camelToSnake(p.label)
      } else {
        p.label
      }

      builder.set(
        fieldName,
        p.typeclass.toAvro(deref, cleanSchema.getField(fieldName).schema()))
      i += 1
    }
    builder.build()
  }

  override def toAvroDefault(v: T, defaultGenericContainer: GenericContainer): Any = {
    val ps = caseClass.parameters
    var i = 0
    val builder = new GenericRecordBuilder(
      AvroElitzurConversionUtils
        .recordToGenericData(defaultGenericContainer.asInstanceOf[GenericRecord]))

    while (i < ps.length) {
      val p = ps(i)
      val deref = p.dereference(v)

      val fieldName = if (defaultGenericContainer.getSchema.getField(p.label) == null) {
        SharedUtils.camelToSnake(p.label)
      } else {
        p.label
      }

      val field = defaultGenericContainer.getSchema.getField(fieldName)
      val containerAsRecord = defaultGenericContainer.asInstanceOf[GenericRecord]

      val fieldValue = if (AvroElitzurConversionUtils.isAvroRecordType(field.schema())) {
        p.typeclass
          .toAvroDefault(deref, containerAsRecord.get(fieldName).asInstanceOf[GenericRecord])
      } else if (AvroElitzurConversionUtils.isAvroArrayType(field.schema())) {
        p.typeclass
          .toAvroDefault(deref, containerAsRecord.get(fieldName).asInstanceOf[GenericArray[_]])
      } else {
        p.typeclass.toAvroDefault(deref, defaultGenericContainer)
      }

      builder.set(fieldName, fieldValue)
      i += 1
    }
    builder.build()
  }
}

@SuppressWarnings(Array("org.wartremover.warts.Var"))
object AvroConverter extends Serializable {
  type Typeclass[T] = AvroConverter[T]

  def combine[T](cc: CaseClass[AvroConverter, T]): AvroConverter[T] = DerivedConverter(cc)

  implicit def gen[T]: AvroConverter[T] = macro ConverterMacros.wrappedRecordConverter[T]
}
