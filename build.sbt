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
import sbt.{Project, addCompilerPlugin, _}
import sbt.librarymanagement.CrossVersion
import com.typesafe.sbt.SbtGit.GitKeys._

// Variables:
val scioVersion = "0.11.4"
val beamVersion = "2.35.0" // must stay in sync with scio
val avroVersion = "1.8.2"
val scalacheckShapelessVersion = "1.2.3"
val scalatestVersion = "3.1.4"
val scalatestMockitoVersion = "3.1.0.0"
val jodaTimeVersion = "2.10.13"
val magnoliaVersion = "1.0.0-M4"
val ratatoolVersion = "0.3.25"
val scalaCheckVersion = "1.14.3"
val enumeratumVersion = "1.7.0"
val scalaCollectionsCompatVersion = "2.6.0"


val disableWarts = Set(
  Wart.NonUnitStatements,
  Wart.Overloading,
  Wart.ImplicitParameter,
  Wart.While,
  Wart.Nothing,
  Wart.AsInstanceOf,
  Wart.Throw,
  Wart.Null,
  Wart.IsInstanceOf,
  // due to BQ macro:
  Wart.DefaultArguments,
  Wart.OptionPartial,
  Wart.Any
)

def isScala213x: Def.Initialize[Boolean] = Def.setting {
  scalaBinaryVersion.value == "2.13"
}

lazy val commonSettings = Defaults.coreDefaultSettings ++ Sonatype.sonatypeSettings ++
  releaseSettings ++ Seq(
  organization          := "com.spotify",
  name                  := "spotify-elitzur",
  scalaVersion          := "2.12.16",
  scalacOptions         ++= Seq(
    "-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked",
    "-language:higherKinds", // Allow higher-kinded types
  ),
//    "-Xlog-implicits"),
  scalacOptions ++= {
    if (isScala213x.value) {
      Seq("-Ymacro-annotations", "-Ywarn-unused")
    } else {
      Seq()
    }
  },
  javacOptions ++= Seq("-source", "1.8",
    "-target", "1.8"),

  // Repositories and dependencies
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "Local Ivy Repository" at "file://" + Path.userHome.absolutePath + "/.ivy2/local",
    "Confluent's Maven Public Repository" at "https://packages.confluent.io/maven/"
  ),

  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",

  crossPaths := true,
  autoScalaLibrary := false,
  crossScalaVersions := Seq("2.12.16", "2.13.6"),

  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
    "org.scalatestplus" %% "mockito-1-10" % scalatestMockitoVersion % "test",
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
    "com.spotify" %% "ratatool-scalacheck" % ratatoolVersion % "test",
    "joda-time" % "joda-time" % jodaTimeVersion,
    "com.softwaremill.magnolia" %% "magnolia-core" % magnoliaVersion,
    "com.beachape" %% "enumeratum" % enumeratumVersion,
    "org.scala-lang.modules" %% "scala-collection-compat" % scalaCollectionsCompatVersion,
  ),
  libraryDependencies ++= {
    if (isScala213x.value) {
      Seq()
    } else {
      Seq(
        compilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
      )
    }
  },

  // Avro files are compiled to src_managed/main/compiled_avro
  // Exclude their parent to avoid confusing IntelliJ
  Compile / sourceDirectories := (Compile / sourceDirectories)
    .value.filterNot(_.getPath.endsWith("/src_managed/main")),
  Compile / managedSourceDirectories := (Compile / managedSourceDirectories)
    .value.filterNot(_.getPath.endsWith("/src_managed/main")),
  Compile / doc / sources := List(),  // suppress warnings
  Compile / wartremoverErrors ++= Warts.unsafe.filterNot(disableWarts.contains),
  compileOrder := CompileOrder.JavaThenScala
)

lazy val scioSettings = Seq(
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-core" % scioVersion,
    "com.spotify" %% "scio-test" % scioVersion % "provided"
  )
)

lazy val noPublishSettings = Seq(
  publish := {},
  publishLocal := {},
  publishArtifact := false
)

lazy val releaseSettings = Seq(
  releaseCrossBuild             := true,
  releasePublishArtifactsAction := PgpKeys.publishSigned.value,
  publishMavenStyle             := true,
  publishArtifact in Test       := false,
  publishTo := Some(if (isSnapshot.value) Opts.resolver.sonatypeSnapshots else Opts.resolver.sonatypeStaging),
  sonatypeProfileName           := "com.spotify",

  licenses := Seq("Apache 2" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  homepage := Some(url("https://github.com/spotify/elitzur")),
  scmInfo := Some(ScmInfo(
    url("https://github.com/spotify/elitzur.git"),
    "scm:git:git@github.com:spotify/elitzur.git")),
  developers := List(
    // current maintainers
    Developer(id="anne-decusatis", name="Anne DeCusatis", email="anned@spotify.com", url=url("https://twitter.com/precisememory")),
    Developer(id="catherinejelder", name="Catherine Elder", email="siege@spotify.com", url=url("https://twitter.com/siegeelder")),
    Developer(id="idreeskhan", name="Idrees Khan", email="me@idrees.xyz", url=url("https://twitter.com/idreesxkhan")),
    Developer(id="jackdingilian", name="Jack Dingilian", email="jackd@spotify.com", url=url("https://github.com/jackdingilian")),
    // TODO add the rest of the team
  )
)

lazy val root: Project = Project(
  "elitzur-validation",
  file(".")
).settings(commonSettings ++ noPublishSettings).aggregate(
  elitzurCore, elitzurAvro, benchmarking, elitzurScio,
  elitzurExamples, elitzurSchemas
)

lazy val elitzurCore: Project = Project(
  "elitzur-core",
  file("elitzur-core")
).settings(
  commonSettings ++ releaseSettings,
  name := "elitzur-core"
)

lazy val elitzurAvro: Project = Project(
  "elitzur-avro",
  file("elitzur-avro")
).settings(
  commonSettings ++ releaseSettings ++ scioSettings,
  name := "elitzur-avro",
  libraryDependencies ++= Seq(
    "com.spotify" %% "scio-avro" % scioVersion
  )
).dependsOn(elitzurCore, elitzurSchemas % Test)

lazy val elitzurScio: Project = Project(
  "elitzur-scio",
  file("elitzur-scio")
).settings(
  commonSettings ++ releaseSettings ++ scioSettings,
  name := "elitzur-scio",
  libraryDependencies ++= Seq(
    "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % scalacheckShapelessVersion % Test,
  ),
  // avro config
  version in AvroConfig := avroVersion,
).dependsOn(elitzurCore % "test->test;compile->compile",
  elitzurAvro
)

lazy val elitzurExamples: Project = Project(
  "elitzur-examples",
  file("elitzur-examples")
).settings(
  // workaround for running examples with Dataflow
  // see related issue https://github.com/spotify/scio/issues/2280
  run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  libraryDependencies ++= Seq(
    "com.spotify" %% "ratatool-scalacheck" % ratatoolVersion,
    "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion
  ),
  commonSettings ++ releaseSettings ++ scioSettings,
  name := "elitzur-examples"
).dependsOn(elitzurCore % "test->test;compile->compile", elitzurAvro,
  elitzurScio, elitzurSchemas)

lazy val benchmarking: Project = Project(
  "benchmarking",
  file("benchmarking")
).enablePlugins(
  JmhPlugin
).settings(
  commonSettings ++ noPublishSettings,
  AvroConfig / version := avroVersion,
  name := "benchmarking",
  libraryDependencies ++= Seq(
    "com.spotify" %% "ratatool-scalacheck" % ratatoolVersion,
  ),
  Test / initialize ~= { _ =>
    // setting this to our mock provider so the tests pass
    System.setProperty("override.type.provider",
      "com.spotify.elitzur.example.ElitzurOverrideTypeProviderTestingImpl")
    // setting this so we can call into BQ for our JobTest
    System.setProperty("bigquery.project", "data-quality-spotify")
  }
).dependsOn(elitzurCore % "test->test;compile->compile",
  elitzurAvro,
  elitzurScio,
  elitzurSchemas)

lazy val elitzurSchemas: Project = Project(
  "elitzur-schemas",
  file("elitzur-schemas")
).settings(
  commonSettings ++ noPublishSettings,
  AvroConfig / version := avroVersion,
  name := "elitzur-schemas"
)

addCommandAlias(
  "verify",
  "; clean; +test; +package; scalastyle; test:scalastyle"
)
