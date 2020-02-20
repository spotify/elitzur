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
