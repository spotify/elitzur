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
package com.spotify.elitzur

import scala.util.matching.Regex

private[elitzur] object Utils {
  val camelRegEx: Regex = "[A-Z\\d]".r

  /**
    * This function exists to handle the disjoint between avro names (snake) and scala names (camel)
    * Finds all upper case chars and replaces with _{char}
    */
  private[elitzur] def camelToSnake(i: String): String = {
    camelRegEx.replaceAllIn(i, { m =>
      if (m.end(0) == 1) {
        m.group(0).toLowerCase()
      } else {
        val s = m.group(0)
        val sb = new StringBuilder(s.length + 1)
        sb.append("_")
        sb.append(s.toLowerCase())
        sb.mkString
      }
    })
  }
}
