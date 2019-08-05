scalaVersion in ThisBuild     := "2.12.8"
scapegoatVersion in ThisBuild := "1.3.9"

val confluent    = "5.3.0"
val kafkaVersion = "2.3.0"
val catsCore     = "2.0.0-M4"
val catsEffect   = "2.0.0-M5"
val catsMtl      = "0.6.0"
val kittens      = "1.2.1"
val circeVersion = "0.12.0-M4"
val fs2Version   = "1.1.0-M1"
val shapeless    = "2.3.3"
val avro         = "2.0.4"
val akkaStream   = "1.0.5"
val fs2Stream    = "0.20.0-M2"
val silencer     = "1.4.2"
val monocle      = "1.5.1-cats" //"1.6.0+50-f7b237d7-SNAPSHOT" //
val contextual   = "1.2.1"
val sparkVersion = "2.4.3"
val avrohugger   = "1.0.0-RC18"
val scalatest    = "3.0.8"
val refined      = "0.9.9"

lazy val commonSettings = Seq(
  version      := "0.0.1-SNAPSHOT",
  organization := "com.github.chenharryhua",
  scalaVersion := scalaVersion.value,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    "Confluent Maven Repo" at "https://packages.confluent.io/maven/"
  ),
  addCompilerPlugin("org.typelevel" %% "kind-projector"  % "0.10.3"),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin(
    "org.scalamacros" %% "paradise" % "2.1.1" cross CrossVersion.full
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

lazy val kafka = (project in file("kafka"))
  .settings(commonSettings: _*)
  .settings(name := "kafka")
  .settings(
    addCompilerPlugin("com.github.ghik" %% "silencer-plugin" % silencer),
    libraryDependencies ++= Seq(
      "com.github.ghik" %% "silencer-lib"         % silencer % Provided,
      "org.scala-lang"                            % "scala-reflect" % scalaVersion.value % Provided,
      "org.scala-lang"                            % "scala-compiler" % scalaVersion.value % Provided,
      "org.apache.kafka"                          % "kafka-clients" % kafkaVersion,
      "org.apache.kafka"                          % "kafka-streams" % kafkaVersion,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaVersion,
      "com.typesafe.akka" %% "akka-stream-kafka"  % akkaStream,
      "com.ovoenergy" %% "fs2-kafka"              % fs2Stream,
      "com.sksamuel.avro4s" %% "avro4s-core"      % avro,
      "io.confluent"                              % "kafka-avro-serializer" % confluent classifier "",
      "com.julianpeeters" %% "avrohugger-core"    % avrohugger,
//json
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion,
//base
      "eu.timepit" %% "refined"                         % refined,
      "org.typelevel" %% "cats-core"                    % catsCore,
      "org.typelevel" %% "cats-mtl-core"                % catsMtl,
      "org.typelevel" %% "kittens"                      % kittens,
      "com.chuusai" %% "shapeless"                      % shapeless,
      "co.fs2" %% "fs2-core"                            % fs2Version,
      "co.fs2" %% "fs2-reactive-streams"                % fs2Version,
      "org.typelevel" %% "cats-effect"                  % catsEffect,
      "com.github.julien-truffaut" %% "monocle-core"    % monocle,
      "com.github.julien-truffaut" %% "monocle-generic" % monocle,
      "com.github.julien-truffaut" %% "monocle-macro"   % monocle,
      "com.github.julien-truffaut" %% "monocle-state"   % monocle,
      "com.github.julien-truffaut" %% "monocle-unsafe"  % monocle,
      "com.propensive" %% "contextual"                  % contextual,
      "dev.zio" %% "zio-interop-cats" % "2.0.0.0-RC1",
      "org.scalatest" %% "scalatest"                    % scalatest % Test
    ),
    excludeDependencies += "javax.ws.rs" % "javax.ws.rs-api"
  )

lazy val sparkafka = (project in file("sparkafka"))
  .dependsOn(kafka)
  .settings(commonSettings: _*)
  .settings(name := "sparkafka")
  .settings(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core"                 % sparkVersion,
      "org.apache.spark" %% "spark-sql"                  % sparkVersion,
      "org.apache.spark" %% "spark-streaming"            % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10"       % sparkVersion,
      "org.apache.spark" %% "spark-avro"                 % sparkVersion
    ),
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.2"
  )
lazy val nanjin = (project in file(".")).aggregate(kafka, sparkafka)
