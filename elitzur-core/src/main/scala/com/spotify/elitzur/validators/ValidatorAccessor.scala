package com.spotify.elitzur.validators

case class ValidatorAccessor[T](validator: Validator[T], value: T, label: String)
