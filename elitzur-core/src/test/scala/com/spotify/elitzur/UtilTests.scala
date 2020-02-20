package com.spotify.elitzur.example

import com.spotify.elitzur.Utils._
import org.scalatest.{FlatSpec, Matchers}

class UtilTests extends FlatSpec with Matchers {
  "camelToSnake" should "convert camelCase to snake_case" in {
    val inputs = Seq("myField", "inputFieldName", "nocamels")

    inputs.map(camelToSnake) should be (Seq("my_field", "input_field_name", "nocamels"))
  }
}
