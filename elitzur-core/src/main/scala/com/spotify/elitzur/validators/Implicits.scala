package com.spotify.elitzur.validators

import scala.collection.mutable
import scala.reflect.ClassTag

trait Implicits {
  //scalastyle:off line.size.limit
  import Validator._

  implicit val stringValidator: FieldValidator[String] = new IgnoreValidator[String]
  implicit val longValidator: FieldValidator[Long] = new IgnoreValidator[Long]
  implicit val doubleValidator: FieldValidator[Double] = new IgnoreValidator[Double]
  implicit val booleanValidator: FieldValidator[Boolean] = new IgnoreValidator[Boolean]
  implicit val arrayByteValidator: FieldValidator[Array[Byte]] = new IgnoreValidator[Array[Byte]]
  implicit val floatValidator: FieldValidator[Float] = new IgnoreValidator[Float]
  implicit val intValidator: FieldValidator[Int] = new IgnoreValidator[Int]

  implicit def baseTypeValidator[T <: BaseValidationType[_]: ClassTag]: FieldValidator[T] = new BaseFieldValidator[T]
  implicit def optionTypeValidator[T <: BaseValidationType[_]: FieldValidator: ClassTag]: FieldValidator[Option[T]] = new OptionTypeValidator[T]
  implicit def statusTypeValidator[T <: BaseValidationType[_]: FieldValidator: ClassTag]: FieldValidator[ValidationStatus[T]] = new StatusTypeValidator[T]
  implicit def statusOptionTypeValidator[T <: BaseValidationType[_]: FieldValidator: ClassTag]: FieldValidator[ValidationStatus[Option[T]]] = new StatusOptionTypeValidator[T]
  implicit def wrappedValidator[T: Validator]: Validator[ValidationStatus[T]] = new WrappedValidator[T]
  implicit def optionValidator[T: Validator]: Validator[Option[T]] = new OptionValidator[T]
  implicit def dynamicTypeValidator[T <: DynamicValidationType[_, _, _]: ClassTag]: DynamicValidator[T] = new DynamicValidator[T]
  implicit def seqValidator[T: Validator: ClassTag]: Validator[Seq[T]] = wrapSeqLikeValidator(() => Seq.newBuilder[T])
  implicit def listValidator[T: Validator: ClassTag]: Validator[List[T]] = wrapSeqLikeValidator(() => List.newBuilder[T])
  implicit def arrayValidator[T: Validator: ClassTag]: Validator[Array[T]] = wrapSeqLikeValidator(() => Array.newBuilder[T])
  implicit def vectorValidator[T: Validator: ClassTag]: Validator[Vector[T]] = wrapSeqLikeValidator(() => Vector.newBuilder[T])
  //scalastyle:on line.size.limit

}
