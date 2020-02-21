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
val scioVersion = "0.8.1"
val avroVersion = "1.8.2"
val scalacheckShapelessVersion = "1.2.3"
val scalatestVersion = "3.1.0"
val scalatestMockitoVersion = "3.1.0.0"
val jodaTimeVersion = "2.10.5"
val magnoliaVersion = "0.12.6"
val ratatoolVersion = "0.3.14"
val scalaCheckVersion = "1.14.3"
val enumeratumVersion = "1.5.14"


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

lazy val commonSettings = Defaults.coreDefaultSettings ++ Seq(
  organization          := "com.spotify",
  name                  := "spotify-elitzur",
  scalaVersion          := "2.12.10",
  version               := {
    val dirtyGit = if (gitUncommittedChanges.value) ".DIRTY" else ""
    val gitHash = gitHeadCommit.value.getOrElse("NOT_GIT").take(7)
    val snapshotSuffix = s"$gitHash$dirtyGit-SNAPSHOT"
    s"0.5.1-$snapshotSuffix"
  },
  scalacOptions         ++= Seq("-target:jvm-1.8",
    "-deprecation",
    "-feature",
    "-unchecked"),
//    "-Xlog-implicits"),
  javacOptions          ++= Seq("-source", "1.8",
    "-target", "1.8"),

  // Repositories and dependencies
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    "Local Maven Repository" at "file://" + Path.userHome.absolutePath + "/.m2/repository",
    "Local Ivy Repository" at "file://" + Path.userHome.absolutePath + "/.ivy2/local"
  ),

  // protobuf-lite is an older subset of protobuf-java and causes issues
  excludeDependencies += "com.google.protobuf" % "protobuf-lite",

  crossPaths := true,
  autoScalaLibrary := false,
  crossScalaVersions := Seq("2.12.8", "2.11.12"),

  addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),

  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scalatest" %% "scalatest" % scalatestVersion % "test",
    "org.scalatestplus" %% "mockito-1-10" % scalatestMockitoVersion % "test",
    "org.scalacheck" %% "scalacheck" % scalaCheckVersion % "test",
    "com.spotify" %% "ratatool-scalacheck" % ratatoolVersion % "test",
    "joda-time" % "joda-time" % jodaTimeVersion,
    {
      // this is what scio does for 2.11 support see https://github.com/spotify/scio/pull/2241
      if (scalaBinaryVersion.value == "2.11") {
        "me.lyh" %% "magnolia" % "0.10.1-jto"
      } else {
        "com.propensive" %% "magnolia" % magnoliaVersion
      }
    },
    "com.beachape" %% "enumeratum" % enumeratumVersion,
  ),

  // Avro files are compiled to src_managed/main/compiled_avro
  // Exclude their parent to avoid confusing IntelliJ
  sourceDirectories in Compile := (sourceDirectories in Compile).value.filterNot(_.getPath.endsWith("/src_managed/main")),
  managedSourceDirectories in Compile := (managedSourceDirectories in Compile).value.filterNot(_.getPath.endsWith("/src_managed/main")),
  sources in doc in Compile := List(),  // suppress warnings
  wartremoverErrors in Compile ++= Warts.unsafe.filterNot(disableWarts.contains),
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
  publishTo in ThisBuild := { // TODO
  None
  },
  publishArtifact := true,
  publishMavenStyle := true,
  publishArtifact in Test := false
)

lazy val root: Project = Project(
  "elitzur-validation",
  file(".")
).settings(commonSettings).aggregate(
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
).dependsOn(elitzurCore)

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
  libraryDependencies ++= Seq(
    "com.spotify" %% "ratatool-scalacheck" % ratatoolVersion,
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
  version in AvroConfig := avroVersion,
  name := "benchmarking",
  libraryDependencies ++= Seq(
    "com.spotify" %% "ratatool-scalacheck" % ratatoolVersion,
  ),
  initialize in Test ~= { _ =>
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
  version in AvroConfig := avroVersion,
  name := "elitzur-schemas"
)

addCommandAlias(
  "verify",
  "; clean; +test; +package; scalastyle; test:scalastyle"
)
