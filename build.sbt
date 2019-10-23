scalaVersion in ThisBuild      := "2.12.10"
scapegoatVersion in ThisBuild  := "1.3.11"
parallelExecution in ThisBuild := false

val confluent    = "5.3.0"
val kafkaVersion = "2.3.0"

val shapeless      = "2.3.3"
val contextual     = "1.2.1"
val kittens        = "2.0.0"
val catsCore       = "2.0.0"
val fs2Version     = "2.0.1"
val catsMtl        = "0.7.0"
val catsTime       = "0.3.0"
val tagless        = "0.10"
val monocleVersion = "2.0.0"
val refined        = "0.9.10"
val droste         = "0.7.0"

val zioCats    = "2.0.0.0-RC6"
val monix      = "3.0.0"
val catsEffect = "2.0.0"

val akkaKafka = "1.1.0"
val fs2Kafka  = "0.20.1"

val sparkVersion     = "2.4.4"
val framelessVersion = "0.8.0"

val circeVersion = "0.12.3"
val jsonDiff     = "4.0.0"

val avro4s     = "3.0.2"
val apacheAvro = "1.9.1"
val avrohugger = "1.0.0-RC19"

val silencer = "1.4.2"
val jline    = "3.13.0"

val scalatest = "3.0.8"

val doobie = "0.8.4"
val quill  = "3.4.10"

val neotypes = "0.13.0"

lazy val commonSettings = Seq(
  version      := "0.0.1-SNAPSHOT",
  organization := "com.github.chenharryhua",
  scalaVersion := scalaVersion.value,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    "Confluent Maven Repo".at("https://packages.confluent.io/maven/")
  ),
  addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.11.0").cross(CrossVersion.full)),
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

val neo4j = Seq(
  "com.dimafeng" %% "neotypes",
  "com.dimafeng" %% "neotypes-cats-effect",
  "com.dimafeng" %% "neotypes-monix",
  "com.dimafeng" %% "neotypes-zio",
  "com.dimafeng" %% "neotypes-akka-stream",
  "com.dimafeng" %% "neotypes-fs2-stream",
  "com.dimafeng" %% "neotypes-monix-stream",
  "com.dimafeng" %% "neotypes-zio-stream",
  "com.dimafeng" %% "neotypes-refined",
  "com.dimafeng" %% "neotypes-cats-data"
).map(_ % neotypes) ++ Seq("org.neo4j.driver" % "neo4j-java-driver" % "1.7.5")

val json = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser"
).map(_                          % circeVersion) ++ Seq(
  "io.circe" %% "circe-optics"   % "0.12.0",
  "org.gnieh" %% "diffson-circe" % jsonDiff)

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
).map(_ % apacheAvro) ++
  Seq(
    "com.sksamuel.avro4s" %% "avro4s-core" % avro4s,
    ("io.confluent" % "kafka-streams-avro-serde" % "5.3.1").classifier(""),
    "com.julianpeeters" %% "avrohugger-core" % avrohugger,
    "io.higherkindness" %% "skeuomorph"      % "0.0.15"
  )

val sparkLib = Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-streaming-kafka-0-10",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-avro",
  "org.apache.spark" %% "spark-graphx"
).map(_ % sparkVersion)
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
  .map(_.exclude("com.sun.jersey.contribs", "jersey-guice"))

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
  "org.typelevel" %% "discipline-scalatest"                   % "1.0.0-RC1"    % Test,
  "com.47deg" %% "scalacheck-toolbox-datetime"                % "0.3.1"        % Test,
  "org.tpolecat" %% "doobie-postgres"                         % doobie         % Test
)

val kafkaLib = Seq(
  "org.apache.kafka" % "kafka-clients",
  "org.apache.kafka" % "kafka-streams",
  "org.apache.kafka" %% "kafka-streams-scala").map(_ % kafkaVersion) ++
  Seq(
    "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafka,
    "com.ovoenergy" %% "fs2-kafka"             % fs2Kafka)

val base = Seq(
  "io.chrisdavenport" %% "cats-time"       % catsTime,
  "eu.timepit" %% "refined"                % refined,
  "org.typelevel" %% "cats-core"           % catsCore,
  "org.typelevel" %% "cats-free"           % catsCore,
  "org.typelevel" %% "alleycats-core"      % catsCore,
  "org.typelevel" %% "cats-mtl-core"       % catsMtl,
  "org.typelevel" %% "kittens"             % kittens,
  "com.propensive" %% "contextual"         % contextual,
  "com.chuusai" %% "shapeless"             % shapeless,
  "io.higherkindness" %% "droste-core"     % droste,
  "org.typelevel" %% "cats-tagless-macros" % tagless
)

val effect = Seq(
  "org.typelevel" %% "cats-effect" % catsEffect,
  "dev.zio" %% "zio-interop-cats"  % zioCats,
  "io.monix" %% "monix"            % monix)

val db = Seq(
  "io.getquill" %% "quill-core"         % quill,
  "io.getquill" %% "quill-codegen-jdbc" % quill,
  "io.getquill" %% "quill-spark"        % quill,
  "org.tpolecat" %% "doobie-core"       % doobie,
  "org.tpolecat" %% "doobie-hikari"     % doobie,
  "org.tpolecat" %% "doobie-quill"      % doobie
)

lazy val codec = (project in file("codec"))
  .settings(commonSettings: _*)
  .settings(name := "codec")
  .settings(
    addCompilerPlugin("com.github.ghik" %% "silencer-plugin" % silencer),
    libraryDependencies ++= Seq(
      "com.github.ghik" %% "silencer-lib" % silencer % Provided
    ) ++ base ++ json ++ monocle ++ kafkaLib ++ avro ++ tests,
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

lazy val database = (project in file("database"))
  .settings(commonSettings: _*)
  .settings(name := "database")
  .settings(libraryDependencies ++= base ++ monocle ++ json ++ db ++ neo4j ++ tests)

lazy val spark = (project in file("spark"))
  .dependsOn(kafka)
  .dependsOn(database)
  .settings(commonSettings: _*)
  .settings(name := "spark")
  .settings(
    libraryDependencies ++= sparkLib ++ frameless ++ tests,
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core"  % "jackson-databind" % "2.6.7.2",
      "org.json4s" %% "json4s-core" % "3.5.5"),
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
  )

lazy val nanjin =
  (project in file("."))
    .settings(name := "nanjin")
    .aggregate(codec, kafka, database, spark)
