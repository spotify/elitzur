package com.spotify.elitzur.scio

import com.spotify.elitzur.validators.{PostValidation, Unvalidated, Validator}
import com.spotify.scio.coders.Coder
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

object ValidatorDoFns {
  class ValidatorDoFn[T: Coder](vr: Validator[T]) extends DoFn[T, T] with
    Serializable {
    @ProcessElement
    def processElement(c: DoFn[T, T]#ProcessContext): Unit = {
      val e = c.element()
      c.output(vr.validateRecord(Unvalidated(e)).forceGet)
    }
  }

  class ValidatorDoFnWithResult[T: Coder](vr: Validator[T]) extends DoFn[T, PostValidation[T]] with
    Serializable {
    @ProcessElement
    def processElement(c: DoFn[T, PostValidation[T]]#ProcessContext): Unit = {
      val x = implicitly[Coder[T]]
      val e = c.element()
      c.output(vr.validateRecord(Unvalidated(e)))
    }
  }
}
