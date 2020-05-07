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
package com.spotify.elitzur.validators

import scala.util.Try
import scala.collection.compat._

private[this] case class PostValidationWrapper[A](inner: A) extends PostValidation[A] {
  override def isValid: Boolean = inner.asInstanceOf[PostValidation[_]].isValid

  override def get: A = inner

  override def map[B](f: A => B): ValidationStatus[B] =
    throw new NotImplementedError("Call forceGet or inner, not implemented")

  override def flatMap[B](f: A => ValidationStatus[B]): ValidationStatus[B] =
    throw new NotImplementedError("Call forceGet or inner, not implemented")

  override def forceGet: A = inner

  override def toOption: Option[A] =
    throw new NotImplementedError("Call forceGet or inner, not implemented")

  override def isNonvalidated: Boolean = inner.asInstanceOf[PostValidation[_]].isNonvalidated

  override def toString: String = inner.toString
}

trait ValidationStatus[+A] extends IterableOnce[A] {
  def isValid: Boolean
  def isNonvalidated: Boolean
  def isPostValidation: Boolean
  def get: A
  def map[B](f: A => B): ValidationStatus[B]
  def flatMap[B](f: A => ValidationStatus[B]): ValidationStatus[B]

  def forceGet: A

  def toOption: Option[A]

  def foreach[U](f: A => U): Unit = f(this.forceGet)

  def isEmpty: Boolean = false

  def hasDefiniteSize: Boolean = true

  def seq: IterableOnce[A] = this

  def forall(p: A => Boolean): Boolean = p(this.forceGet)

  def exists(p: A => Boolean): Boolean = p(this.forceGet)

  def find(p: A => Boolean): Option[A] =
    if (p(this.forceGet)) Some(this.forceGet) else None

  def copyToArray[B >: A](xs: Array[B], start: Int, len: Int): Unit =
    xs.update(start, this.forceGet)

  def toTraversable: Iterable[A] = this.asInstanceOf[Iterable[A]]

  def isTraversableAgain: Boolean = true

  def toStream: Stream[A] = Stream(this.forceGet)

  def toIterator: Iterator[A] = this.iterator

  def iterator: Iterator[A] = Iterator(this.forceGet)

}

abstract class PreValidation[+A] extends ValidationStatus[A] {
  def isPostValidation: Boolean = false

  def isInvalid: Boolean = !isValid
}

abstract class PostValidation[+A] extends ValidationStatus[A] {
  def isPostValidation: Boolean = true

  def isInvalid: Boolean = !isValid

  def isNonvalidated: Boolean
}

case class Unvalidated[+A](x: A) extends PreValidation[A] {
  override def isValid: Boolean = false

  override def get: A = throw new Exception("Can't get Unvalidated data, use getOpt")
  def getOpt: Option[A] = Some(x)

  override def isNonvalidated: Boolean = false

  override def forceGet: A = x

  override def map[B](f: A => B): ValidationStatus[B] = Unvalidated(f(x))

  override def flatMap[B](f: A => ValidationStatus[B]): ValidationStatus[B] = f(x)

  override def toOption: Option[A] = Some(x)

  //TODO: Remove this, should not be saving unvalidated data, Only added for simple benchmarking
  override def toString: String = x.toString
}

case class Valid[+A](x: A) extends PostValidation[A] {
  def isValid: Boolean = true
  def get: A = x

  override def forceGet: A = x

  override def map[B](f: A => B): ValidationStatus[B] = Valid(f(this.x))

  override def flatMap[B](f: A => ValidationStatus[B]): ValidationStatus[B] = f(this.x)

  override def toOption: Option[A] = Some(x)

  override def isNonvalidated: Boolean = false

  override def toString: String = x.toString
}

final case class IgnoreValidation[+A](a: A) extends PostValidation[A] {
  override def isNonvalidated: Boolean = true

  override def isValid: Boolean = true

  override def get: A = a

  override def map[B](f: A => B): ValidationStatus[B] = IgnoreValidation(f(a))

  override def flatMap[B](f: A => ValidationStatus[B]): ValidationStatus[B] = f(a)

  override def toOption: Option[A] = Some(a)

  override def forceGet: A = a
}

final case class Invalid[+A](x: A) extends PostValidation[A] {
  override def isValid: Boolean = false
  override def get: A = throw new Exception("Can't get Invalid data, use getInvalid")
  def getOpt: Option[A] = Try(recover(identity)).toOption
  def getInvalid: A = x
  override def map[B](f: A => B): ValidationStatus[B] = Invalid(f(x))
  override def flatMap[B](f: A => ValidationStatus[B]): ValidationStatus[B] = f(x)
  def recover[B](f: A => B): B = f(x)
  def recoverValid[B](f: A => B): Valid[B] = Valid(recover(f))

  override def toString: String = x.toString

  override def toOption: Option[A] = None

  override def forceGet: A = x

  override def isNonvalidated: Boolean = false

  // Override this so flatten and flatMap skip Invalid records
  override def iterator: Iterator[A] = Iterator()
}
