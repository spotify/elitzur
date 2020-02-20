package com.spotify.elitzur

trait MetricsReporter extends Serializable {
  def reportValid(className: String, fieldName: String, validationTypeName: String): Unit
  def reportInvalid(className: String, fieldName: String, validationTypeName: String): Unit
}
