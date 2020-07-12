scalaVersion in ThisBuild      := "2.12.11"
scapegoatVersion in ThisBuild  := "1.3.11"
parallelExecution in ThisBuild := false

version in ThisBuild := "0.8.0-SNAPSHOT"

// generic
val shapeless  = "2.3.3"
val contextual = "1.2.1"
val kittens    = "2.1.0"
val catsCore   = "2.1.1"
val algebra    = "2.0.1"
val fs2Version = "2.4.2"
val streamz    = "0.13-RC1"
val catsMtl    = "0.7.1"
val catsTime   = "0.3.0"
val tagless    = "0.11"
val monocle    = "2.0.5"
val refined    = "0.9.14"
val droste     = "0.8.0"
val enumeratum = "1.6.1"
val chimney    = "0.5.2"

// runtime
val zioCats    = "2.1.3.0-RC16"
val monix      = "3.2.2"
val catsEffect = "2.1.3"
val akka26     = "2.6.7"

// kafka
val kafka25   = "2.5.0"
val akkaKafka = "2.0.3"
val fs2Kafka  = "1.0.0"

// spark
val spark24   = "2.4.6"
val frameless = "0.8.0"

// database
val doobie   = "0.9.0"
val quill    = "3.5.2"
val neotypes = "0.14.0"
val elastic  = "7.8.0"

// format
val circe   = "0.13.0"
val jackson = "2.10.4"
val kantan  = "0.6.1"
val parquet = "1.11.0"
val avro    = "1.10.0"
val avro4s  = "3.1.1"

// connect
val hadoop  = "3.2.1"
val akkaFtp = "2.0.1"

// misc
val silencer    = "1.7.0"
val jline       = "3.15.0"
val log4s       = "1.8.2"
val betterFiles = "3.9.1"

// test
val scalatest = "3.2.0"

// deprecate ?
val flink110 = "1.11.0"

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
    "-Xfuture"
  ),
  Test / classLoaderLayeringStrategy  := ClassLoaderLayeringStrategy.Flat,
  bloopExportJarClassifiers in Global := Some(Set("sources"))
)

val hadoopLib = Seq(
  "org.apache.hadoop" % "hadoop-aws",
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-client",
  "org.apache.hadoop" % "hadoop-hdfs").map(_ % hadoop) ++
  Seq("com.amazonaws" % "aws-java-sdk-bundle" % "1.11.818")

val flinkLib = Seq(
  "org.apache.flink" %% "flink-connector-kafka",
  "org.apache.flink" %% "flink-streaming-scala",
  "org.apache.flink" %% "flink-gelly",
  "org.apache.flink" %% "flink-cep",
  "org.apache.flink" %% "flink-parquet",
  // "org.apache.flink" %% "flink-jdbc",
  "org.apache.flink" %% "flink-hadoop-compatibility",
  "org.apache.flink" % "flink-s3-fs-hadoop"
).map(_ % flink110)

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
).map(_ % neotypes) ++ Seq("org.neo4j.driver" % "neo4j-java-driver" % "1.7.5")

val jsonLib = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-shapes",
  "io.circe" %% "circe-jawn",
  "io.circe" %% "circe-optics",
  "io.circe" %% "circe-jackson210"
).map(_ % circe)

val kantanLib = Seq(
  "com.nrinaudo" %% "kantan.csv",
  "com.nrinaudo" %% "kantan.csv-java8",
  "com.nrinaudo" %% "kantan.csv-generic",
  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantan)

val avroLib = Seq(
  "org.apache.avro"                        % "avro"                      % avro,
  "org.apache.avro"                        % "avro-compiler"             % avro,
  "io.confluent"                           % "kafka-streams-avro-serde"  % "5.5.1",
  "org.apache.parquet"                     % "parquet-avro"              % parquet,
  "com.julianpeeters" %% "avrohugger-core" % "1.0.0-RC21",
  "com.sksamuel.avro4s" %% "avro4s-core"   % avro4s,
  "io.confluent"                           % "kafka-protobuf-serializer" % "5.5.1"
)

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
).map(_ % spark24) ++ Seq(
  "org.typelevel" %% "frameless-dataset",
  "org.typelevel" %% "frameless-ml",
  "org.typelevel" %% "frameless-cats"
).map(_ % frameless)

val testLib = Seq(
  "org.typelevel" %% "cats-testkit-scalatest"                 % "1.0.1"   % Test,
  "org.typelevel" %% "discipline-scalatest"                   % "1.0.1"   % Test,
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
  "org.apache.kafka" %% "kafka-streams-scala").map(_ % kafka25) ++
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
    "org.typelevel" %% "cats-mtl-core"       % catsMtl,
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
  "io.getquill" %% "quill-codegen-jdbc",
  "io.getquill" %% "quill-spark"
).map(_ % quill)

val doobieLib = Seq(
  "org.tpolecat" %% "doobie-core",
  "org.tpolecat" %% "doobie-hikari",
  "org.tpolecat" %% "doobie-quill"
).map(_ % doobie)

val dbLib = doobieLib ++ quillLib

lazy val common = (project in file("common"))
  .settings(commonSettings: _*)
  .settings(name := "nj-common")
  .settings(
    libraryDependencies ++= Seq(
      "org.jline"               % "jline" % jline,
      "com.lihaoyi" %% "pprint" % "0.5.9") ++
      baseLib ++ fs2Lib ++ monocleLib ++ testLib)

lazy val datetime = (project in file("datetime"))
  .settings(commonSettings: _*)
  .settings(name := "nj-datetime")
  .settings(libraryDependencies ++= baseLib ++ monocleLib ++ testLib)

lazy val messages = (project in file("messages"))
  .settings(commonSettings: _*)
  .settings(name := "nj-messages")
  .settings(libraryDependencies ++= Seq(
    compilerPlugin(("com.github.ghik" % "silencer-plugin" % silencer).cross(CrossVersion.full)),
    ("com.github.ghik"                % "silencer-lib"    % silencer % Provided).cross(CrossVersion.full)
  ) ++ baseLib ++ fs2Lib ++ avroLib ++ jsonLib ++ kafkaLib ++ monocleLib ++ testLib)

lazy val devices = (project in file("devices"))
  .settings(commonSettings: _*)
  .settings(name := "nj-devices")
  .settings(
    libraryDependencies ++=
      Seq("com.lightbend.akka" %% "akka-stream-alpakka-ftp" % akkaFtp) ++
        baseLib ++ fs2Lib ++ hadoopLib ++ avroLib ++ effectLib ++ akkaLib ++ testLib)

lazy val pipes = (project in file("pipes"))
  .settings(commonSettings: _*)
  .settings(name := "nj-pipes")
  .settings(libraryDependencies ++=
    baseLib ++ fs2Lib ++ effectLib ++ kantanLib ++ jsonLib ++ avroLib ++ testLib)

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

lazy val database = (project in file("database"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-database")
  .settings(
    libraryDependencies ++= baseLib ++ jsonLib ++ dbLib ++ neotypesLib ++ elastic4sLib ++ testLib)

lazy val spark = (project in file("spark"))
  .dependsOn(kafka)
  .dependsOn(database)
  .dependsOn(devices)
  .settings(commonSettings: _*)
  .settings(name := "nj-spark")
  .settings(
    libraryDependencies ++= Seq(
      "com.github.pathikrit" %% "better-files" % betterFiles,
      "org.locationtech.jts"                   % "jts-core" % "1.17.0",
      "org.log4s" %% "log4s"                   % log4s) ++
      sparkLib ++ avroLib ++ hadoopLib ++ testLib,
    dependencyOverrides ++= Seq(
      "com.fasterxml.jackson.core"                             % "jackson-databind" % jackson,
      "com.fasterxml.jackson.core"                             % "jackson-core"     % jackson,
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % jackson,
      "org.json4s" %% "json4s-core"                            % "3.5.5"
    )
  )

lazy val flink = (project in file("flink"))
  .dependsOn(kafka)
  .settings(commonSettings: _*)
  .settings(name := "nj-flink")
  .settings(libraryDependencies ++= flinkLib ++ testLib)

lazy val nanjin =
  (project in file("."))
    .settings(name := "nanjin")
    .aggregate(common, messages, datetime, devices, pipes, kafka, flink, database, spark)
