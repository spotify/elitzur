package com.spotify.elitzur

import com.spotify.elitzur.validators.featureflags.FeatureFlag
import org.slf4j.LoggerFactory

import java.io.InputStream
import java.util.Properties
import scala.util.{Failure, Using}

object Properties {
  case object ValidationErrorContext extends FeatureFlag {
    override def key: String = "validation.errorContext"
  }

  private val logger = LoggerFactory.getLogger(this.getClass)
  val props = new Properties
  Using(
    classOf[Nothing].getClassLoader
      .getResourceAsStream("elitzur.properties")
  ) { input: InputStream =>
    props.load(input)
  } match {
    case Failure(exception) =>
      logger.error("Failed to load elitzur config", exception)
    case _ => ()
  }

}
