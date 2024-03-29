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
package com.spotify.elitzur.example

import com.spotify.elitzur.Utils._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UtilTests extends AnyFlatSpec with Matchers {
  "camelToSnake" should "convert camelCase to snake_case" in {
    val inputs = Seq("myField", "inputFieldName", "nocamels")

    inputs.map(camelToSnake) should be (Seq("my_field", "input_field_name", "nocamels"))
  }
}
