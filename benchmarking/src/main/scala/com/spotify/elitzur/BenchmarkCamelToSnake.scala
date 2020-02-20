package com.spotify.elitzur

import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations.{Benchmark, BenchmarkMode, Mode, OutputTimeUnit}

class BenchmarkCamelToSnake {
  @Benchmark @BenchmarkMode(Array(Mode.Throughput)) @OutputTimeUnit(TimeUnit.MILLISECONDS)
  def camelToSnake(): String = {
    Utils.camelToSnake("testingInputCamelCaseStringForFunction")
  }
}
