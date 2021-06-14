scalaVersion in ThisBuild      := "2.12.14"
scapegoatVersion in ThisBuild  := "1.3.11"
parallelExecution in ThisBuild := false
cancelable in Global           := true

version in ThisBuild := "0.12.12-SNAPSHOT"

// generic
val shapeless  = "2.3.7"
val contextual = "1.2.1"
val kittens    = "2.3.2"
val catsCore   = "2.6.1"
val algebra    = "2.2.3"
val fs2Version = "3.0.4"
val catsMtl    = "1.2.1"
val catsTime   = "0.3.4"
val tagless    = "0.14.0"
val monocle    = "2.1.0"
val refined    = "0.9.26"
val droste     = "0.8.0"
val enumeratum = "1.6.1"
val chimney    = "0.6.1"

// runtime
val zioCats    = "3.1.1.0"
val monix      = "3.4.0"
val catsEffect = "3.1.1"
val akka26     = "2.6.15"

// kafka
val akkaKafka = "2.1.0"
val fs2Kafka  = "2.1.0"

// spark
val spark3    = "3.1.2"
val frameless = "0.10.1"

// database
val doobie   = "1.0.0-M5"
val quill    = "3.7.1"
val neotypes = "0.17.0"

// format
val jackson = "2.12.3"
val json4s  = "3.7.0-M7" // for spark
val kantan  = "0.6.1"
val parquet = "1.12.0"
val avro    = "1.10.2"
val avro4s  = "4.0.9"

// connect
val hadoop  = "3.3.0"
val akkaFtp = "3.0.0"
val http4s  = "1.0.0-M23"

// misc
val silencer    = "1.7.5"
val log4s       = "1.8.2"
val betterFiles = "3.9.1"

// test
val scalatest = "3.2.9"

lazy val commonSettings = Seq(
  organization := "com.github.chenharryhua",
  scalaVersion := scalaVersion.value,
  resolvers ++= Seq(
    Resolver.sonatypeRepo("public"),
    Resolver.sonatypeRepo("releases"),
    "Confluent Maven Repo".at("https://packages.confluent.io/maven/")
  ),
  addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.0").cross(CrossVersion.full)),
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
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  Test / PB.targets                  := Seq(scalapb.gen() -> (Test / sourceManaged).value)
)

val awsLib = Seq("com.amazonaws" % "aws-java-sdk-bundle" % "1.11.999")

val hadoopLib = Seq(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core",
  "org.apache.hadoop" % "hadoop-aws",
  "org.apache.hadoop" % "hadoop-auth",
  "org.apache.hadoop" % "hadoop-annotations",
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-client",
  "org.apache.hadoop" % "hadoop-hdfs"
).map(_ % hadoop) ++ awsLib

val neotypesLib = Seq(
  "com.dimafeng" %% "neotypes",
  "com.dimafeng" %% "neotypes-cats-effect"
).map(_ % neotypes) ++ Seq("org.neo4j.driver" % "neo4j-java-driver" % "4.3.1")

val circeLib = Seq(
  "io.circe" %% "circe-core"           % "0.14.1",
  "io.circe" %% "circe-generic"        % "0.14.1",
  "io.circe" %% "circe-parser"         % "0.14.1",
  "io.circe" %% "circe-shapes"         % "0.14.1",
  "io.circe" %% "circe-jawn"           % "0.14.1",
  "io.circe" %% "circe-optics"         % "0.14.1",
  "io.circe" %% "circe-jackson210"     % "0.14.0",
  "io.circe" %% "circe-generic-extras" % "0.14.1"
)

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
  "org.json4s" %% "json4s-native",
  "org.json4s" %% "json4s-scalap"
).map(_ % json4s)

val kantanLib = Seq(
  "com.nrinaudo" %% "kantan.csv",
  "com.nrinaudo" %% "kantan.csv-java8",
  "com.nrinaudo" %% "kantan.csv-generic",
  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantan) ++ Seq("com.nrinaudo" %% "kantan.codecs" % "0.5.2")

val pbLib = Seq(
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.3",
  "com.google.protobuf"                       % "protobuf-java"             % "3.17.3",
  "com.google.protobuf"                       % "protobuf-java-util"        % "3.17.3",
  "io.confluent"                              % "kafka-protobuf-serializer" % "6.2.0"
)

val serdeLib = Seq(
  "com.sksamuel.avro4s" %% "avro4s-core" % avro4s,
  "org.apache.parquet"                   % "parquet-common"           % parquet,
  "org.apache.parquet"                   % "parquet-hadoop"           % parquet,
  "org.apache.parquet"                   % "parquet-avro"             % parquet,
  "org.apache.avro"                      % "avro"                     % avro,
  "io.confluent"                         % "kafka-streams-avro-serde" % "6.2.0"
) ++ jacksonLib ++ circeLib ++ pbLib

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

val sparkLib = Seq(
  "org.apache.spark" %% "spark-catalyst",
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
  "org.typelevel" %% "frameless-cats",
  "org.typelevel" %% "frameless-core"
).map(_ % frameless) ++ Seq(
  "org.apache.avro" % "avro-compiler",
  "org.apache.avro" % "avro-mapred"
).map(_ % avro)

val testLib = Seq(
  "org.typelevel" %% "cats-effect-testkit"                    % catsEffect      % Test,
  "org.typelevel" %% "cats-testkit-scalatest"                 % "2.1.5"         % Test,
  "org.typelevel" %% "discipline-scalatest"                   % "2.1.5"         % Test,
  "org.typelevel" %% "cats-laws"                              % catsCore        % Test,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.14" % "1.2.5"         % Test,
  "org.scalatest" %% "scalatest"                              % scalatest       % Test,
  "com.github.julien-truffaut" %% "monocle-law"               % monocle         % Test,
  "com.47deg" %% "scalacheck-toolbox-datetime"                % "0.6.0"         % Test,
  "org.tpolecat" %% "doobie-postgres"                         % doobie          % Test,
  "com.typesafe.akka" %% "akka-stream-testkit"                % akka26          % Test,
  "org.typelevel" %% "algebra-laws"                           % algebra         % Test,
  "com.typesafe.akka" %% "akka-stream-kafka-testkit"          % akkaKafka       % Test,
  "org.slf4j"                                                 % "slf4j-log4j12" % "1.7.30" % Test,
  "log4j"                                                     % "log4j"         % "1.2.17" % Test
)

val kafkaLib = Seq(
  "org.apache.kafka" % "kafka-clients",
  "org.apache.kafka" % "kafka-streams",
  "org.apache.kafka" %% "kafka-streams-scala").map(_ % "6.2.0-ce") ++
  Seq("com.typesafe.akka" %% "akka-stream-kafka"     % akkaKafka, "com.github.fd4s" %% "fs2-kafka" % fs2Kafka) ++
  Seq("io.confluent" % "kafka-schema-registry-client", "io.confluent" % "kafka-schema-serializer").map(_ % "6.2.0")

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
  "org.typelevel" %% "cats-kernel",
  "org.typelevel" %% "cats-core",
  "org.typelevel" %% "cats-free",
  "org.typelevel" %% "alleycats-core"
).map(_ % catsCore) ++
  Seq(
    "org.typelevel" %% "cats-mtl"            % catsMtl,
    "org.typelevel" %% "kittens"             % kittens,
    "org.typelevel" %% "cats-tagless-macros" % tagless,
    "org.typelevel" %% "algebra"             % algebra
  )

val refinedLib = Seq(
  "eu.timepit" %% "refined",
  "eu.timepit" %% "refined-cats"
).map(_ % refined)

val baseLib = Seq(
  "org.typelevel" %% "squants"                     % "1.8.0",
  "org.scala-lang.modules" %% "scala-java8-compat" % "1.0.0",
  "org.typelevel" %% "case-insensitive"            % "1.1.4",
  "io.scalaland" %% "chimney"                      % chimney,
  "io.scalaland" %% "enumz"                        % "1.0.0",
  "com.twitter" %% "algebird-core"                 % "0.13.8",
  "com.propensive" %% "contextual"                 % contextual,
  "com.chuusai" %% "shapeless"                     % shapeless
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
  "dev.zio" %% "zio"               % "1.0.9" % Provided,
  "dev.zio" %% "zio-interop-cats"  % zioCats % Provided,
  "io.monix" %% "monix-eval"       % monix   % Provided,
  "io.monix" %% "monix"            % monix   % Provided
)

val quillLib = Seq(
  "io.getquill" %% "quill-core",
  "io.getquill" %% "quill-codegen",
  "io.getquill" %% "quill-codegen-jdbc"
).map(_ % quill)

val doobieLib = Seq(
  "org.tpolecat" %% "doobie-core",
  "org.tpolecat" %% "doobie-hikari",
  "org.tpolecat" %% "doobie-free",
  "org.tpolecat" %% "doobie-quill"
).map(_ % doobie) ++ Seq("com.zaxxer" % "HikariCP" % "4.0.3")

val logLib = Seq(
  "org.log4s" %% "log4s" % "1.10.0",
  "org.slf4j"            % "slf4j-api" % "1.7.30"
)

val ftpLib = Seq(
  "commons-net"                                     % "commons-net" % "3.8.0",
  "com.hierynomus"                                  % "sshj"        % "0.31.0",
  "com.lightbend.akka" %% "akka-stream-alpakka-ftp" % akkaFtp
)

val http4sLib = Seq(
  "org.http4s" %% "http4s-blaze-server",
  "org.http4s" %% "http4s-blaze-client",
  "org.http4s" %% "http4s-circe",
  "org.http4s" %% "http4s-dsl"
).map(_ % http4s)

val dbLib = doobieLib ++ quillLib ++ neotypesLib

lazy val common = (project in file("common"))
  .settings(commonSettings: _*)
  .settings(name := "nj-common")
  .settings(libraryDependencies ++= Seq("org.apache.commons" % "commons-lang3" % "3.12.0") ++
    baseLib ++ fs2Lib ++ effectLib ++ monocleLib ++ logLib ++ testLib)

lazy val salesforce = (project in file("salesforce"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-salesforce")
  .settings(
    libraryDependencies ++=
      Seq("com.github.cb372" %% "cats-retry" % "3.0.0") ++
        http4sLib ++ fs2Lib ++ effectLib ++ logLib ++ circeLib ++ baseLib ++ monocleLib ++ testLib)

lazy val aws = (project in file("aws"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-aws")
  .settings(
    libraryDependencies ++=
      Seq(
        "com.typesafe.akka" %% "akka-http"                % "10.2.4",
        "com.lightbend.akka" %% "akka-stream-alpakka-sqs" % "3.0.1"
      ) ++ akkaLib ++ circeLib ++ baseLib ++ monocleLib ++ testLib ++ awsLib.map(_ % Provided))

lazy val datetime = (project in file("datetime"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-datetime")
  .settings(
    libraryDependencies ++= Seq(
      "com.lihaoyi" %% "fastparse"       % "2.3.2",
      "io.chrisdavenport" %% "cats-time" % catsTime) ++
      baseLib ++ monocleLib ++ testLib)

lazy val guard = (project in file("guard"))
  .dependsOn(aws)
  .dependsOn(datetime)
  .settings(commonSettings: _*)
  .settings(name := "nj-guard")
  .settings(
    libraryDependencies ++= Seq(
      "com.github.cb372" %% "cats-retry" % "3.0.0",
      "eu.timepit" %% "fs2-cron-cron4s"  % "0.7.1",
      "io.dropwizard.metrics"            % "metrics-core" % "4.2.0") ++
      logLib ++ circeLib ++ baseLib ++ monocleLib ++ testLib ++ awsLib.map(_ % Provided))

lazy val messages = (project in file("messages"))
  .settings(commonSettings: _*)
  .settings(name := "nj-messages")
  .settings(libraryDependencies ++= Seq(
    compilerPlugin(("com.github.ghik" % "silencer-plugin" % silencer).cross(CrossVersion.full)),
    ("com.github.ghik"                % "silencer-lib"    % silencer % Provided).cross(CrossVersion.full)
  ) ++ baseLib ++ effectLib ++ fs2Lib ++ serdeLib ++ kafkaLib ++ monocleLib ++ testLib)

lazy val pipes = (project in file("pipes"))
  .settings(commonSettings: _*)
  .settings(name := "nj-pipes")
  .settings(libraryDependencies ++=
    baseLib ++ fs2Lib ++ effectLib ++ kantanLib ++ ftpLib ++ akkaLib ++ hadoopLib ++ serdeLib ++ testLib)

lazy val database = (project in file("database"))
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-database")
  .settings(libraryDependencies ++= baseLib ++ fs2Lib ++ effectLib ++ monocleLib ++ dbLib ++ testLib)

lazy val kafka = (project in file("kafka"))
  .dependsOn(messages)
  .dependsOn(datetime)
  .dependsOn(common)
  .settings(commonSettings: _*)
  .settings(name := "nj-kafka")
  .settings(
    libraryDependencies ++= baseLib ++ fs2Lib ++ serdeLib ++ effectLib ++ monocleLib ++
      kafkaLib ++ akkaLib ++ logLib ++ testLib,
    excludeDependencies += "javax.ws.rs" % "javax.ws.rs-api"
  )

lazy val spark = (project in file("spark"))
  .dependsOn(kafka)
  .dependsOn(pipes)
  .dependsOn(database)
  .settings(commonSettings: _*)
  .settings(name := "nj-spark")
  .settings(
    libraryDependencies ++= Seq(
      "org.locationtech.jts"                   % "jts-core" % "1.18.1",
      "com.github.pathikrit" %% "better-files" % betterFiles,
      // for spark
      "io.getquill" %% "quill-spark"               % quill,
      "com.thesamet.scalapb" %% "sparksql-scalapb" % "0.11.0",
      // override dependency
      "io.netty"                               % "netty"      % "3.10.6.Final",
      "io.netty"                               % "netty-all"  % "4.1.65.Final",
      "com.julianpeeters" %% "avrohugger-core" % "1.0.0-RC24" % Test
    ) ++ baseLib ++ sparkLib ++ serdeLib ++ kantanLib ++ hadoopLib ++ kafkaLib ++ effectLib ++
      akkaLib ++ json4sLib ++ fs2Lib ++ monocleLib ++ dbLib ++ logLib ++ ftpLib ++ testLib,
    excludeDependencies ++= Seq(ExclusionRule(organization = "io.netty"))
  )

lazy val example = (project in file("example"))
  .dependsOn(spark)
  .dependsOn(guard)
  .settings(commonSettings: _*)
  .settings(name := "nj-example")
  .settings(libraryDependencies ++= testLib)

lazy val nanjin =
  (project in file("."))
    .settings(name := "nanjin")
    .aggregate(common, datetime, salesforce, aws, guard, messages, pipes, kafka, database, spark)
