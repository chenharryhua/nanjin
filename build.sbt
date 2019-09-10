scalaVersion in ThisBuild      := "2.12.9"
scapegoatVersion in ThisBuild  := "1.3.10"
parallelExecution in ThisBuild := false

val confluent    = "5.3.0"
val kafkaVersion = "2.3.0"

val shapeless      = "2.3.3"
val contextual     = "1.2.1"
val kittens        = "2.0.0"
val catsCore       = "2.0.0"
val catsEffect     = "2.0.0"
val fs2Version     = "2.0.0"
val catsMtl        = "0.6.0"
val catsTime       = "0.3.0-M1"
val monocleVersion = "2.0.0-RC1"
val refined        = "0.9.9"
val zioCats        = "2.0.0.0-RC3"

val akkaKafka = "1.0.5"
val fs2Kafka  = "0.20.0-RC1"

val sparkVersion     = "2.4.4"
val framelessVersion = "0.8.0"

val circeVersion = "0.12.1"
val avro4s       = "3.0.1"
val avroVersion  = "1.9.1"
val avrohugger   = "1.0.0-RC19"

val silencer = "1.4.2"
val jline    = "3.12.1"

val scalatest = "3.0.8"

lazy val commonSettings = Seq(
  version      := "0.0.1-SNAPSHOT",
  organization := "com.github.chenharryhua",
  scalaVersion := scalaVersion.value,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    "Confluent Maven Repo".at("https://packages.confluent.io/maven/")
  ),
  addCompilerPlugin("org.typelevel" %% "kind-projector"  % "0.10.3"),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin(("org.scalamacros" %% "paradise" % "2.1.1").cross(CrossVersion.full)),
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect"  % scalaVersion.value % Provided,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided
  ),
  scalacOptions ++= Seq(
    "-Ypartial-unification",
    "-deprecation",
    "-encoding",
    "UTF-8",
    "-feature",
    "-language:existentials",
    "-language:higherKinds",
    "-unchecked",
    "-Xfatal-warnings",
    //  "-Xlint",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Xfuture"
  )
)

val circe = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_ % circeVersion)

val fs2 = Seq(
  "co.fs2" %% "fs2-core",
  "co.fs2" %% "fs2-reactive-streams",
  "co.fs2" %% "fs2-io"
).map(_ % fs2Version)

val monocle = Seq(
  "com.github.julien-truffaut" %% "monocle-core",
  "com.github.julien-truffaut" %% "monocle-generic",
  "com.github.julien-truffaut" %% "monocle-macro",
  "com.github.julien-truffaut" %% "monocle-state",
  "com.github.julien-truffaut" %% "monocle-unsafe"
).map(_ % monocleVersion)

val avro = Seq(
  "org.apache.avro" % "avro",
  "org.apache.avro" % "avro-mapred",
  "org.apache.avro" % "avro-compiler",
  "org.apache.avro" % "avro-ipc"
).map(_ % avroVersion) ++
  Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % avro4s,
    ("io.confluent" % "kafka-streams-avro-serde" % confluent).classifier(""),
    "com.julianpeeters" %% "avrohugger-core" % avrohugger
  )

val spark = Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-streaming-kafka-0-10",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-avro"
).map(_ % sparkVersion)

val frameless = Seq(
  "org.typelevel" %% "frameless-dataset",
  "org.typelevel" %% "frameless-ml",
  "org.typelevel" %% "frameless-cats"
).map(_ % framelessVersion)

val tests = Seq(
  "org.typelevel" %% "cats-laws"                              % catsCore       % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.13" % "1.1.8"        % Test,
  "org.scalatest" %% "scalatest"                              % scalatest      % Test,
  "com.github.julien-truffaut" %% "monocle-law"               % monocleVersion % Test,
  "org.typelevel" %% "discipline-scalatest"                   % "1.0.0-M1"     % Test
)

val kafkaLib = Seq(
  "org.apache.kafka" % "kafka-clients",
  "org.apache.kafka" % "kafka-streams",
  "org.apache.kafka" %% "kafka-streams-scala").map(_ % kafkaVersion) ++
  Seq(
    "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafka,
    "com.ovoenergy" %% "fs2-kafka"             % fs2Kafka
  )

val base = Seq(
  "io.chrisdavenport" %% "cats-time"  % catsTime,
  "eu.timepit" %% "refined"           % refined,
  "org.typelevel" %% "cats-core"      % catsCore,
  "org.typelevel" %% "cats-free"      % catsCore,
  "org.typelevel" %% "alleycats-core" % catsCore,
  "org.typelevel" %% "cats-mtl-core"  % catsMtl,
  "org.typelevel" %% "kittens"        % kittens,
  "com.propensive" %% "contextual"    % contextual,
  "com.chuusai" %% "shapeless"        % shapeless
)

val effect = Seq(
  "org.typelevel" %% "cats-effect" % catsEffect,
  "dev.zio" %% "zio-interop-cats"  % zioCats
)

lazy val codec = (project in file("codec"))
  .settings(commonSettings: _*)
  .settings(name := "codec")
  .settings(
    addCompilerPlugin("com.github.ghik" %% "silencer-plugin" % silencer),
    libraryDependencies ++= Seq(
      "com.github.ghik" %% "silencer-lib" % silencer % Provided
    ) ++ base ++ kafkaLib ++ circe ++ monocle ++ avro ++ tests,
    excludeDependencies += "javax.ws.rs" % "javax.ws.rs-api"
  )

lazy val kafka = (project in file("kafka"))
  .settings(commonSettings: _*)
  .settings(name := "kafka")
  .dependsOn(codec)
  .settings(
    libraryDependencies ++= Seq("org.jline" % "jline" % jline) ++ effect ++ fs2 ++ tests,
    excludeDependencies += "javax.ws.rs" % "javax.ws.rs-api"
  )

lazy val sparkafka = (project in file("sparkafka"))
  .dependsOn(kafka)
  .settings(commonSettings: _*)
  .settings(name := "sparkafka")
  .settings(
    libraryDependencies ++= (spark ++ frameless ++ tests)
      .map(_.exclude("io.netty", "netty-buffer"))
      .map(_.exclude("io.netty", "netty-codec"))
      .map(_.exclude("io.netty", "netty-codec-http"))
      .map(_.exclude("io.netty", "netty-common"))
      .map(_.exclude("io.netty", "netty-handler"))
      .map(_.exclude("io.netty", "netty-resolver"))
      .map(_.exclude("io.netty", "netty-transport"))
      .map(_.exclude("com.sun.jersey", "jersey-client"))
      .map(_.exclude("com.sun.jersey", "jersey-core"))
      .map(_.exclude("com.sun.jersey", "jersey-json"))
      .map(_.exclude("com.sun.jersey", "jersey-server"))
      .map(_.exclude("com.sun.jersey", "jersey-servlet"))
      .map(_.exclude("com.sun.jersey.contribs", "jersey-guice")),
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core"  % "jackson-databind" % "2.6.7.2",
      "org.json4s" %% "json4s-core" % "3.5.5")
//    excludeDependencies += "javax.ws.rs"                % "javax.ws.rs-api"
  )
lazy val nanjin = (project in file(".")).aggregate(codec, kafka, sparkafka)
