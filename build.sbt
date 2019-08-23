scalaVersion in ThisBuild      := "2.12.9"
scapegoatVersion in ThisBuild  := "1.3.10"
parallelExecution in ThisBuild := false

val confluent    = "5.3.0"
val kafkaVersion = "2.3.0"
val catsCore     = "2.0.0-RC1"
val catsEffect   = "2.0.0-RC1"
val catsMtl      = "0.6.0"
val kittens      = "2.0.0-M1"
val circeVersion = "0.12.0-RC3"
val fs2Version   = "1.1.0-M1"
val shapeless    = "2.3.3"
val avro4s       = "3.0.0"
val avro         = "1.9.0"
val avrohugger   = "1.0.0-RC18"
val akkaStream   = "1.0.5"
val fs2Stream    = "0.20.0-M2"
val silencer     = "1.4.2"
val monocle      = "2.0.0-RC1"
val contextual   = "1.2.1"
val sparkVersion = "2.4.3"
val scalatest    = "3.0.8"
val refined      = "0.9.9"
val zioCats      = "2.0.0.0-RC2"
val frameless    = "0.8.0"
val jline        = "3.12.1"

lazy val commonSettings = Seq(
  version      := "0.0.2-SNAPSHOT",
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

lazy val sparkafka = (project in file("kafka"))
  .settings(commonSettings: _*)
  .settings(name := "sparkafka")
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
//avro
      "org.apache.avro"                      % "avro" % avro,
      "org.apache.avro"                      % "avro-mapred" % avro,
      "org.apache.avro"                      % "avro-compiler" % avro,
      "org.apache.avro"                      % "avro-ipc" % avro,
      "com.sksamuel.avro4s" %% "avro4s-core" % avro4s,
      ("io.confluent" % "kafka-streams-avro-serde" % confluent).classifier(""),
      "com.julianpeeters" %% "avrohugger-core" % avrohugger,
//json
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion,
//base
      "eu.timepit" %% "refined"                          % refined,
      "org.typelevel" %% "cats-core"                     % catsCore,
      "org.typelevel" %% "alleycats-core"                % catsCore,
      "org.typelevel" %% "cats-mtl-core"                 % catsMtl,
      "org.typelevel" %% "kittens"                       % kittens,
      "com.chuusai" %% "shapeless"                       % shapeless,
      "co.fs2" %% "fs2-core"                             % fs2Version,
      "co.fs2" %% "fs2-reactive-streams"                 % fs2Version,
      "co.fs2" %% "fs2-io"                               % fs2Version,
      "org.typelevel" %% "cats-effect"                   % catsEffect,
      "com.github.julien-truffaut" %% "monocle-core"     % monocle,
      "com.github.julien-truffaut" %% "monocle-generic"  % monocle,
      "com.github.julien-truffaut" %% "monocle-macro"    % monocle,
      "com.github.julien-truffaut" %% "monocle-state"    % monocle,
      "com.github.julien-truffaut" %% "monocle-unsafe"   % monocle,
      "com.propensive" %% "contextual"                   % contextual,
      "dev.zio" %% "zio-interop-cats"                    % zioCats,
      "org.jline"                                        % "jline" % jline,
      "org.apache.spark" %% "spark-core"                 % sparkVersion,
      "org.apache.spark" %% "spark-sql"                  % sparkVersion,
      "org.apache.spark" %% "spark-streaming"            % sparkVersion,
      "org.apache.spark" %% "spark-streaming-kafka-0-10" % sparkVersion,
      "org.apache.spark" %% "spark-sql-kafka-0-10"       % sparkVersion,
      "org.apache.spark" %% "spark-avro"                 % sparkVersion,
      "org.typelevel" %% "frameless-dataset"             % frameless,
      "org.typelevel" %% "frameless-ml"                  % frameless,
      "org.typelevel" %% "frameless-cats"                % frameless,
      "org.scalatest" %% "scalatest"                     % scalatest % Test
    ).map(_.exclude("io.netty", "netty-buffer"))
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
    dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7.2",
    excludeDependencies += "javax.ws.rs"                % "javax.ws.rs-api"
  )
/*
lazy val sparkafka = (project in file("sparkafka"))
  .dependsOn(nj_kafka)
  .settings(commonSettings: _*)
  .settings(name := "sparkafka")
  .settings(
    libraryDependencies ++= Seq("org.scalatest" %% "scalatest" % scalatest % Test)
  )
*/
lazy val nanjin = (project in file(".")).aggregate(sparkafka)
