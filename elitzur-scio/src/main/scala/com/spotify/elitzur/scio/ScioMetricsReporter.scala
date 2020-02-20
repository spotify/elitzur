package com.spotify.elitzur.scio

import com.spotify.elitzur.{CounterTypes, MetricsReporter}

class ScioMetricsReporter extends MetricsReporter {
  override def reportValid(className: String, fieldName: String, validationType: String): Unit =
    ElitzurMetrics.getCounter(className, fieldName, validationType, CounterTypes.Valid).inc()

  override def reportInvalid(className: String, fieldName: String, validationType: String): Unit =
    ElitzurMetrics.getCounter(className, fieldName, validationType, CounterTypes.Invalid).inc()
}
