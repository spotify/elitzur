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

import com.spotify.elitzur.converters.avro.dynamic.validator.core.{
  ArrayValidatorOp,
  OptionValidatorOp,
  ValidatorOp,
  ValidatorOpsUtil
}
import com.spotify.elitzur.converters.avro.dynamic.dsl.core.{
  ArrayFlatmapAccessor,
  ArrayMapAccessor,
  ArrayNoopAccessor,
  BaseAccessor,
  IndexAccessor,
  NullableAccessor
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class AccessorOpToValidatorOpTest extends AnyFlatSpec with Matchers {
  val noOpFn: Any => Any = (o: Any) => o

  it should "single index accessor should correctly parse" in {
    val accessors: List[BaseAccessor] = List(IndexAccessor(noOpFn))
    ValidatorOpsUtil.toValidatorOp(accessors) should be (List.empty[ValidatorOp])
  }

  it should "subsequent nullable accessor should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](NullableAccessor(noOpFn,
        List[BaseAccessor](IndexAccessor(noOpFn), NullableAccessor(noOpFn,
          List[BaseAccessor](IndexAccessor(noOpFn))
        ))
      ))
    ValidatorOpsUtil.toValidatorOp(accessors) should be (List(OptionValidatorOp))
  }

  it should "map and nullable accessors should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](ArrayNoopAccessor(noOpFn,
        List[BaseAccessor](IndexAccessor(noOpFn), NullableAccessor(noOpFn,
          List[BaseAccessor](IndexAccessor(noOpFn))
        )),
      flatten = false))
    ValidatorOpsUtil.toValidatorOp(accessors) should be (List(ArrayValidatorOp, OptionValidatorOp))
  }

  it should "only the first map should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](ArrayFlatmapAccessor(noOpFn,
        List[BaseAccessor](IndexAccessor(noOpFn), NullableAccessor(noOpFn,
          List[BaseAccessor](IndexAccessor(noOpFn), ArrayMapAccessor(noOpFn,
            List[BaseAccessor](IndexAccessor(noOpFn))))
        ))
      ))
    ValidatorOpsUtil.toValidatorOp(accessors) should be (List(ArrayValidatorOp, OptionValidatorOp))
  }

  it should "null accessors separated by a map accessor should correctly parse" in {
    val accessors: List[BaseAccessor] =
      List[BaseAccessor](NullableAccessor(noOpFn,
        List[BaseAccessor](IndexAccessor(noOpFn), NullableAccessor(noOpFn,
          List[BaseAccessor](IndexAccessor(noOpFn), ArrayMapAccessor(noOpFn,
            List[BaseAccessor](IndexAccessor(noOpFn), NullableAccessor(noOpFn,
              List[BaseAccessor](IndexAccessor(noOpFn))
            ))))
        ))
      ))
    ValidatorOpsUtil.toValidatorOp(accessors) should be (
      List(OptionValidatorOp, ArrayValidatorOp, OptionValidatorOp))
  }

}
