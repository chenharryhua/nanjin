scalaVersion in ThisBuild      := "2.12.12"
scapegoatVersion in ThisBuild  := "1.3.11"
parallelExecution in ThisBuild := false
cancelable in Global           := true

version in ThisBuild := "0.9.0-SNAPSHOT"

// generic
val shapeless  = "2.3.3"
val contextual = "1.2.1"
val kittens    = "2.1.0"
val catsCore   = "2.2.0"
val algebra    = "2.0.1"
val fs2Version = "2.4.4"
val streamz    = "0.13-RC1"
val catsMtl    = "1.0.0"
val catsTime   = "0.3.4"
val tagless    = "0.11"
val monocle    = "2.1.0"
val refined    = "0.9.16"
val droste     = "0.8.0"
val enumeratum = "1.6.1"
val chimney    = "0.5.3"

// runtime
val zioCats    = "2.1.4.0"
val monix      = "3.2.2"
val catsEffect = "2.2.0"
val akka26     = "2.6.9"

// kafka
val confltKafka = "6.0.0-ce"
val confluent   = "6.0.0"
val akkaKafka   = "2.0.5"
val fs2Kafka    = "1.0.0"

// spark
val spark3   = "3.0.1"
val frameless = "0.9.0-SNAPSHOT"

// database
val doobie   = "0.9.2"
val quill    = "3.5.3"
val neotypes = "0.15.1"
val elastic  = "7.9.1"

// format
val circe   = "0.13.0"
val jackson = "2.11.2"
val json4s  = "3.7.0-M6"  // for spark
val kantan  = "0.6.1"
val parquet = "1.11.1"
val avro    = "1.10.0"
val avro4s  = "4.0.0"

// connect
val hadoop  = "3.3.0"
val akkaFtp = "2.0.2"

// misc
val silencer    = "1.7.1"
val jline       = "3.16.0"
val log4s       = "1.8.2"
val betterFiles = "3.9.1"

// test
val scalatest = "3.2.2"

lazy val commonSettings = Seq(
  organization := "com.github.chenharryhua",
  scalaVersion := scalaVersion.value,
  resolvers ++= Seq(
    Resolver.bintrayRepo("streamz", "maven"),
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
    "-Yrangepos",
    "-Yno-adapted-args",
    "-Ywarn-dead-code",
    "-Ywarn-numeric-widen",
    "-Ywarn-value-discard",
    "-Xfuture"
  ),
  Test / classLoaderLayeringStrategy  := ClassLoaderLayeringStrategy.Flat,
  PB.targets in Test                  := Seq(scalapb.gen() -> (sourceManaged in Test).value),
  bloopExportJarClassifiers in Global := Some(Set("sources"))
)

val hadoopLib = Seq(
  "org.apache.hadoop" % "hadoop-aws",
  "org.apache.hadoop" % "hadoop-auth",
  "org.apache.hadoop" % "hadoop-annotations",
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-client",
  "org.apache.hadoop" % "hadoop-hdfs"
).map(_               % hadoop) ++
  Seq("com.amazonaws" % "aws-java-sdk-bundle" % "1.11.870")

val neotypesLib = Seq(
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
).map(_ % neotypes) ++ Seq("org.neo4j.driver" % "neo4j-java-driver" % "4.1.1")

val circeLib = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-shapes",
  "io.circe" %% "circe-jawn",
  "io.circe" %% "circe-optics",
  "io.circe" %% "circe-jackson210",
  "io.circe" %% "circe-generic-extras"
).map(_ % circe)

val jacksonLib = Seq(
  "com.fasterxml.jackson.core"     % "jackson-annotations",
  "com.fasterxml.jackson.core"     % "jackson-core",
  "com.fasterxml.jackson.core"     % "jackson-databind",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8",
  "com.fasterxml.jackson.module"   % "jackson-module-jaxb-annotations",
  "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-base",
  "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-json-provider",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"
).map(_ % jackson)

val json4sLib = Seq(
  "org.json4s" %% "json4s-ast",
  "org.json4s" %% "json4s-core",
  "org.json4s" %% "json4s-jackson",
  "org.json4s" %% "json4s-navtive",
  "org.json4s" %% "json4s-scalap"
).map(_ % json4s)

val kantanLib = Seq(
  "com.nrinaudo" %% "kantan.csv",
  "com.nrinaudo" %% "kantan.csv-java8",
  "com.nrinaudo" %% "kantan.csv-generic",
  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantan)

val serdeLib = Seq(
  "org.apache.avro"                           % "avro"                      % avro,
  "org.apache.avro"                           % "avro-compiler"             % avro,
  "org.apache.avro"                           % "avro-mapred"               % avro,
  "org.apache.parquet"                        % "parquet-avro"              % parquet,
  "io.confluent"                              % "kafka-streams-avro-serde"  % confluent,
  "io.confluent"                              % "kafka-protobuf-serializer" % confluent,
  "com.sksamuel.avro4s" %% "avro4s-core"      % avro4s,
  "io.higherkindness" %% "skeuomorph"         % "0.0.25",
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.10.8",
  "com.google.protobuf"                       % "protobuf-java"             % "3.13.0",
  "com.google.protobuf"                       % "protobuf-java-util"        % "3.13.0"
) ++ jacksonLib ++ circeLib

val fs2Lib = Seq(
  "co.fs2" %% "fs2-core",
  "co.fs2" %% "fs2-reactive-streams",
  "co.fs2" %% "fs2-io"
).map(_ % fs2Version)

val monocleLib = Seq(
  "com.github.julien-truffaut" %% "monocle-core",
  "com.github.julien-truffaut" %% "monocle-generic",
  "com.github.julien-truffaut" %% "monocle-macro",
  "com.github.julien-truffaut" %% "monocle-state",
  "com.github.julien-truffaut" %% "monocle-unsafe"
).map(_ % monocle)

val elastic4sLib = Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core"
).map(_ % elastic)

val sparkLib = Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-streaming-kafka-0-10",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-avro",
  "org.apache.spark" %% "spark-graphx"
).map(_ % spark3) ++ Seq(
  "org.typelevel" %% "frameless-dataset",
  "org.typelevel" %% "frameless-ml",
  "org.typelevel" %% "frameless-cats"
).map(_ % frameless)

val testLib = Seq(
  "org.typelevel" %% "cats-testkit-scalatest"                 % "2.0.0"   % Test,
  "org.typelevel" %% "discipline-scalatest"                   % "2.0.1"   % Test,
  "org.typelevel" %% "cats-laws"                              % catsCore  % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"   % Test,
  "org.scalatest" %% "scalatest"                              % scalatest % Test,
  "com.github.julien-truffaut" %% "monocle-law"               % monocle   % Test,
  "com.47deg" %% "scalacheck-toolbox-datetime"                % "0.3.5"   % Test,
  "org.tpolecat" %% "doobie-postgres"                         % doobie    % Test,
  "com.typesafe.akka" %% "akka-stream-testkit"                % akka26    % Test,
  "org.typelevel" %% "algebra-laws"                           % algebra   % Test,
  "com.typesafe.akka" %% "akka-stream-kafka-testkit"          % akkaKafka % Test
)

val kafkaLib = Seq(
  "org.apache.kafka" % "kafka-clients",
  "org.apache.kafka" % "kafka-streams",
  "org.apache.kafka" %% "kafka-streams-scala").map(_ % confltKafka) ++
  Seq(
    "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafka,
    "com.github.fd4s" %% "fs2-kafka"           % fs2Kafka)

val enumLib = Seq(
  "com.beachape" %% "enumeratum-cats",
  "com.beachape" %% "enumeratum"
).map(_ % enumeratum)

val drosteLib = Seq(
  "io.higherkindness" %% "droste-core",
  "io.higherkindness" %% "droste-macros",
  "io.higherkindness" %% "droste-meta"
).map(_ % droste)

val catsLib = Seq(
  "org.typelevel" %% "cats-core",
  "org.typelevel" %% "cats-free",
  "org.typelevel" %% "alleycats-core"
).map(_ % catsCore) ++
  Seq(
    "org.typelevel" %% "cats-mtl"           % catsMtl,
    "org.typelevel" %% "kittens"             % kittens,
    "org.typelevel" %% "cats-tagless-macros" % tagless,
    "org.typelevel" %% "algebra"             % algebra
  )

val refinedLib = Seq(
  "eu.timepit" %% "refined",
  "eu.timepit" %% "refined-cats"
).map(_ % refined)

val baseLib = Seq(
  "com.github.krasserm" %% "streamz-converter" % streamz,
  "io.scalaland" %% "chimney"                  % chimney,
  "io.scalaland" %% "enumz"                    % "1.0.0",
  "com.twitter" %% "algebird-core"             % "0.13.7",
  "io.chrisdavenport" %% "cats-time"           % catsTime,
  "com.propensive" %% "contextual"             % contextual,
  "com.chuusai" %% "shapeless"                 % shapeless
) ++ enumLib ++ drosteLib ++ catsLib ++ refinedLib

val akkaLib = Seq(
  "com.typesafe.akka" %% "akka-actor-typed",
  "com.typesafe.akka" %% "akka-actor",
  "com.typesafe.akka" %% "akka-protobuf",
  "com.typesafe.akka" %% "akka-slf4j",
  "com.typesafe.akka" %% "akka-stream-typed",
  "com.typesafe.akka" %% "akka-stream"
).map(_ % akka26)

val effectLib = Seq(
  "org.typelevel" %% "cats-effect" % catsEffect,
  "dev.zio" %% "zio-interop-cats"  % zioCats,
  "io.monix" %% "monix"            % monix)

val quillLib = Seq(
  "io.getquill" %% "quill-core",
  "io.getquill" %% "quill-codegen-jdbc"
).map(_ % quill)

val doobieLib = Seq(
  "org.tpolecat" %% "doobie-core",
  "org.tpolecat" %% "doobie-hikari",
  "org.tpolecat" %% "doobie-quill"
).map(_ % doobie)

val dbLib = doobieLib ++ quillLib ++ neotypesLib ++ elastic4sLib

lazy val common = (project in file("common"))
  .settings(commonSettings: _*)
  .settings(name := "nj-common")
  .settings(
    libraryDependencies ++= Seq(
      "org.log4s" %% "log4s"    % log4s,
      "com.lihaoyi" %% "pprint" % "0.6.0",
      "org.jline"               % "jline" % jline) ++
      baseLib ++ fs2Lib ++ monocleLib ++ testLib)

lazy val datetime = (project in file("datetime"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-datetime")
  .settings(libraryDependencies ++= baseLib ++ monocleLib ++ testLib)

lazy val messages = (project in file("messages"))
  .settings(commonSettings: _*)
  .settings(name := "nj-messages")
  .settings(libraryDependencies ++= Seq(
    compilerPlugin(("com.github.ghik" % "silencer-plugin" % silencer).cross(CrossVersion.full)),
    ("com.github.ghik"                % "silencer-lib"    % silencer % Provided).cross(CrossVersion.full)
  ) ++ baseLib ++ fs2Lib ++ serdeLib ++ kafkaLib ++ monocleLib ++ testLib)

lazy val devices = (project in file("devices"))
  .settings(commonSettings: _*)
  .settings(name := "nj-devices")
  .settings(
    libraryDependencies ++=
      Seq("com.lightbend.akka" %% "akka-stream-alpakka-ftp" % akkaFtp) ++
        baseLib ++ fs2Lib ++ hadoopLib ++ serdeLib ++ effectLib ++ akkaLib ++ testLib)

lazy val pipes = (project in file("pipes"))
  .settings(commonSettings: _*)
  .settings(name := "nj-pipes")
  .settings(libraryDependencies ++=
    baseLib ++ fs2Lib ++ effectLib ++ kantanLib ++ serdeLib ++ testLib)

lazy val database = (project in file("database"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-database")
  .settings(libraryDependencies ++= baseLib ++ dbLib ++ testLib)

lazy val kafka = (project in file("kafka"))
  .dependsOn(messages)
  .dependsOn(pipes)
  .dependsOn(datetime)
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-kafka")
  .settings(
    libraryDependencies ++= effectLib ++ kafkaLib ++ akkaLib ++ testLib,
    excludeDependencies += "javax.ws.rs" % "javax.ws.rs-api"
  )

lazy val spark = (project in file("spark"))
  .dependsOn(kafka)
  .dependsOn(database)
  .dependsOn(devices)
  .settings(commonSettings: _*)
  .settings(name := "nj-spark")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.jts"                   % "jts-core" % "1.17.1",
      "com.github.pathikrit" %% "better-files" % betterFiles,
      // for spark
      "io.getquill" %% "quill-spark"               % quill,
      "com.thesamet.scalapb" %% "sparksql-scalapb" % "0.10.4",
      // override dependency
      "io.netty" % "netty"     % "3.10.6.Final",
      "io.netty" % "netty-all" % "4.1.52.Final",
      "com.julianpeeters" %% "avrohugger-core" % "1.0.0-RC21" % Test
    ) ++ sparkLib ++ serdeLib ++ hadoopLib ++ testLib,
    excludeDependencies ++= Seq(ExclusionRule(organization = "io.netty")),
    dependencyOverrides ++= json4sLib
  )

lazy val nanjin =
  (project in file("."))
    .settings(name := "nanjin")
    .aggregate(common, messages, datetime, devices, pipes, kafka, database, spark)
