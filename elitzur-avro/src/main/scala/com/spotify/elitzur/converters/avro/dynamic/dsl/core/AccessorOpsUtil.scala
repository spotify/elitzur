package com.spotify.elitzur.converters.avro.dynamic.dsl.core

object AccessorOpsUtil {

    def combineFns(accessors: List[BaseAccessor]): Any => Any =
      accessors.map(_.fn).reduceLeftOption((f, g) => f andThen g).getOrElse(NoopAccessor().fn)

}
