/*
 * Copyright 2021 Spotify AB.
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
package com.spotify.elitzur

import com.spotify.elitzur.converters.avro.dynamic.{
  ArrayValidatorOp,
  OptionValidatorOp,
  ValidatorOp
}
import com.spotify.elitzur.converters.avro.dynamic.dsl.{
  ArrayFlatmapAccessor,
  ArrayMapAccessor,
  ArrayNoopAccessor,
  BaseAccessor,
  FieldAccessor,
  IndexAccessor,
  NullableAccessor
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AccessorOpToValidatorOpTest extends AnyFlatSpec with Matchers {
  val DEFAULT_VALUE = ""

  it should "single index accessor should correctly parse" in {
    val accessors: List[BaseAccessor] = List(IndexAccessor(DEFAULT_VALUE))
    FieldAccessor(accessors).toValidatorOp should be (List.empty[ValidatorOp])
  }

  it should "subsequent nullable accessor should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](NullableAccessor(DEFAULT_VALUE,
        List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), NullableAccessor(DEFAULT_VALUE,
          List[BaseAccessor](IndexAccessor(DEFAULT_VALUE))
        ))
      ))
    FieldAccessor(accessors).toValidatorOp should be (List(OptionValidatorOp))
  }

  it should "map and nullable accessors should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](ArrayNoopAccessor(DEFAULT_VALUE,
        List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), NullableAccessor(DEFAULT_VALUE,
          List[BaseAccessor](IndexAccessor(DEFAULT_VALUE))
        )),
      ))
    FieldAccessor(accessors).toValidatorOp should be (List(ArrayValidatorOp, OptionValidatorOp))
  }

  it should "only the first map should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](ArrayFlatmapAccessor(DEFAULT_VALUE,
        List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), NullableAccessor(DEFAULT_VALUE,
          List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), ArrayMapAccessor(DEFAULT_VALUE,
            List[BaseAccessor](IndexAccessor(DEFAULT_VALUE))))
        ))
      ))
    FieldAccessor(accessors).toValidatorOp should be (List(ArrayValidatorOp, OptionValidatorOp))
  }

  it should "null accessors separated by a map accessor should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](NullableAccessor(DEFAULT_VALUE,
        List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), NullableAccessor(DEFAULT_VALUE,
          List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), ArrayMapAccessor(DEFAULT_VALUE,
            List[BaseAccessor](IndexAccessor(DEFAULT_VALUE), NullableAccessor(DEFAULT_VALUE,
              List[BaseAccessor](IndexAccessor(DEFAULT_VALUE))
            ))))
        ))
      ))
    FieldAccessor(accessors).toValidatorOp should be (
      List(OptionValidatorOp, ArrayValidatorOp, OptionValidatorOp))
  }

}
