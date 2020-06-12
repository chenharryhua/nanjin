scalaVersion in ThisBuild      := "2.12.11"
scapegoatVersion in ThisBuild  := "1.3.11"
parallelExecution in ThisBuild := false

version in ThisBuild := "0.7.0-SNAPSHOT"

val confluent    = "5.3.0"
val kafkaVersion = "2.5.0"

val shapeless  = "2.3.3"
val contextual = "1.2.1"
val kittens    = "2.1.0"
val catsCore   = "2.1.1"
val fs2Version = "2.4.1"
val catsMtl    = "0.7.1"
val catsTime   = "0.3.0"
val tagless    = "0.11"
val monocle    = "2.0.4"
val refined    = "0.9.14"
val droste     = "0.8.0"

val zioCats    = "2.1.3.0-RC15"
val monix      = "3.2.2"
val catsEffect = "2.1.3"

val akka = "2.6.6"

val akkaKafka = "2.0.3"

val sparkVersion = "2.4.6"
val frameless    = "0.8.0"

val circe         = "0.13.0"
val kantanVersion = "0.6.1"

val silencerVersion = "1.7.0"
val jline           = "3.15.0"

val scalatest = "3.1.2"

val doobie = "0.9.0"
val quill  = "3.5.1"

val neotypes = "0.13.3"

val flinkVersion = "1.10.1"

val hadoopVersion = "3.2.1"

val awsVersion = "1.11.800"

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
  "org.apache.hadoop" % "hadoop-hdfs").map(_ % hadoopVersion) ++
  Seq("com.amazonaws" % "aws-java-sdk-bundle" % awsVersion)

val flinkLib = Seq(
  "org.apache.flink" %% "flink-connector-kafka",
  "org.apache.flink" %% "flink-streaming-scala",
  "org.apache.flink" %% "flink-gelly",
  "org.apache.flink" %% "flink-cep",
  "org.apache.flink" %% "flink-parquet",
  "org.apache.flink" %% "flink-jdbc",
  "org.apache.flink" %% "flink-hadoop-compatibility",
  "org.apache.flink" % "flink-s3-fs-hadoop"
).map(_ % flinkVersion)

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
).map(_                  % neotypes) ++
  Seq("org.neo4j.driver" % "neo4j-java-driver" % "1.7.5")

val jsonLib = Seq(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-shapes",
  "io.circe" %% "circe-jawn",
  "io.circe" %% "circe-optics",
  "io.circe" %% "circe-jackson210"
).map(_ % circe)

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

val avroLib = Seq(
  "org.apache.avro"                        % "avro"                     % "1.9.2",
  "org.apache.avro"                        % "avro-compiler"            % "1.9.2",
  "io.confluent"                           % "kafka-streams-avro-serde" % "5.5.0",
  "org.apache.parquet"                     % "parquet-avro"             % "1.11.0",
  "com.julianpeeters" %% "avrohugger-core" % "1.0.0-RC21",
  "com.sksamuel.avro4s" %% "avro4s-core"   % "3.1.1"
)

val elastic4sLib = Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core"
).map(_ % "7.7.0")

val sparkLib = Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-streaming-kafka-0-10",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-avro",
  "org.apache.spark" %% "spark-graphx"
).map(_ % sparkVersion)

val framelessLib = Seq(
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
  "com.typesafe.akka" %% "akka-stream-testkit"                % akka      % Test,
  "org.typelevel" %% "algebra-laws"                           % "2.0.1"   % Test,
  "com.typesafe.akka" %% "akka-stream-kafka-testkit"          % akkaKafka % Test
)

val kafkaLib = Seq(
  "org.apache.kafka" % "kafka-clients",
  "org.apache.kafka" % "kafka-streams",
  "org.apache.kafka" %% "kafka-streams-scala").map(_ % kafkaVersion) ++
  Seq(
    "com.typesafe.akka" %% "akka-stream-kafka" % akkaKafka,
    "com.github.fd4s" %% "fs2-kafka"           % "1.0.0"
  )

val baseLib = Seq(
  "com.github.krasserm" %% "streamz-converter" % "0.13-RC1",
  "io.scalaland" %% "chimney"                  % "0.5.2",
  "com.beachape" %% "enumeratum-cats"          % "1.6.1",
  "com.beachape" %% "enumeratum"               % "1.6.1",
  "com.twitter" %% "algebird-core"             % "0.13.7",
  "org.typelevel" %% "algebra"                 % "2.0.1",
  "io.chrisdavenport" %% "cats-time"           % catsTime,
  "eu.timepit" %% "refined"                    % refined,
  "eu.timepit" %% "refined-cats"               % refined,
  "org.typelevel" %% "cats-core"               % catsCore,
  "org.typelevel" %% "cats-free"               % catsCore,
  "org.typelevel" %% "alleycats-core"          % catsCore,
  "org.typelevel" %% "cats-mtl-core"           % catsMtl,
  "org.typelevel" %% "kittens"                 % kittens,
  "com.propensive" %% "contextual"             % contextual,
  "com.chuusai" %% "shapeless"                 % shapeless,
  "io.higherkindness" %% "droste-core"         % droste,
  "io.higherkindness" %% "droste-macros"       % droste,
  "io.higherkindness" %% "droste-meta"         % droste,
  "org.typelevel" %% "cats-tagless-macros"     % tagless
)

val akkaLib = Seq(
  "com.typesafe.akka" %% "akka-actor-typed",
  "com.typesafe.akka" %% "akka-actor",
  "com.typesafe.akka" %% "akka-protobuf",
  "com.typesafe.akka" %% "akka-slf4j",
  "com.typesafe.akka" %% "akka-stream-typed",
  "com.typesafe.akka" %% "akka-stream"
).map(_ % akka)

val effectLib = Seq(
  "org.typelevel" %% "cats-effect" % catsEffect,
  "dev.zio" %% "zio-interop-cats"  % zioCats,
  "io.monix" %% "monix"            % monix)

val dbLib = Seq(
  "io.getquill" %% "quill-core"         % quill,
  "io.getquill" %% "quill-codegen-jdbc" % quill,
  "io.getquill" %% "quill-spark"        % quill,
  "org.tpolecat" %% "doobie-core"       % doobie,
  "org.tpolecat" %% "doobie-hikari"     % doobie,
  "org.tpolecat" %% "doobie-quill"      % doobie
)

val kantanLib = Seq(
  "com.nrinaudo" %% "kantan.csv",
  "com.nrinaudo" %% "kantan.csv-java8",
  "com.nrinaudo" %% "kantan.csv-generic",
  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantanVersion)

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

lazy val devices = (project in file("devices"))
  .settings(commonSettings: _*)
  .settings(name := "nj-devices")
  .settings(
    libraryDependencies ++=
      Seq("com.lightbend.akka" %% "akka-stream-alpakka-ftp" % "2.0.1") ++
        baseLib ++ fs2Lib ++ hadoopLib ++ avroLib ++ effectLib ++ akkaLib ++ testLib)

lazy val pipes = (project in file("pipes"))
  .settings(commonSettings: _*)
  .settings(name := "nj-pipes")
  .settings(libraryDependencies ++=
    baseLib ++ fs2Lib ++ effectLib ++ jsonLib ++ kantanLib ++ avroLib ++ testLib)

lazy val kafka = (project in file("kafka"))
  .dependsOn(pipes)
  .dependsOn(datetime)
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-kafka")
  .settings(
    libraryDependencies ++= Seq(
      compilerPlugin(
        ("com.github.ghik" % "silencer-plugin" % silencerVersion).cross(CrossVersion.full)),
      ("com.github.ghik"   % "silencer-lib"    % silencerVersion % Provided).cross(CrossVersion.full)
    ) ++ effectLib ++ kafkaLib ++ akkaLib ++ avroLib ++ jsonLib ++ testLib,
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
  .dependsOn(pipes)
  .settings(commonSettings: _*)
  .settings(name := "nj-spark")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.jts" % "jts-core" % "1.16.1",
      "org.log4s" %% "log4s" % "1.8.2") ++
      sparkLib ++ avroLib ++ framelessLib ++ hadoopLib ++ testLib,
    dependencyOverrides ++= Seq(
      "io.netty"                                               % "netty-all"        % "4.1.17.Final",
      "io.netty"                                               % "netty"            % "3.9.9.Final",
      "com.fasterxml.jackson.core"                             % "jackson-databind" % "2.10.2",
      "com.fasterxml.jackson.core"                             % "jackson-core"     % "2.10.2",
      "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.10.2",
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
    .aggregate(common, datetime, devices, pipes, kafka, flink, database, spark)
