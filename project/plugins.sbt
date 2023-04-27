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
addSbtPlugin("com.cavorite" % "sbt-avro-1-8" % "1.1.9")
addSbtPlugin("com.etsy" % "sbt-checkstyle-plugin" % "3.1.1")
addSbtPlugin("org.scalastyle" %% "scalastyle-sbt-plugin" % "1.0.0")
addSbtPlugin("pl.project13.scala" % "sbt-jmh" % "0.4.3")
addSbtPlugin("org.wartremover" % "sbt-wartremover" % "2.4.16")
addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "1.0.2")
addSbtPlugin("com.github.sbt" % "sbt-release" % "1.1.0")
addSbtPlugin("org.xerial.sbt" % "sbt-sonatype" % "3.9.10")
addSbtPlugin("com.github.sbt" % "sbt-pgp" % "2.1.2")

libraryDependencies ++= Seq(
  "com.spotify.checkstyle" % "spotify-checkstyle-config" % "1.0.10",
  "com.puppycrawl.tools" % "checkstyle" % "10.10.0"
)
