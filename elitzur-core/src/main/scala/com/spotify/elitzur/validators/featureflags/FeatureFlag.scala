package com.spotify.elitzur.validators.featureflags

import com.spotify.elitzur.validators.{
  DefaultFieldConfig,
  FeatureEnabled,
  ValidationRecordConfig
}

object FeatureFlag {

  val ValidationErrorContext = "ElitzurFeatureFlag/ValidationErrorContext"

  def isEnabled(feature: String, conf: ValidationRecordConfig): Boolean =
    conf.fieldConfig(feature) == FeatureEnabled

}
