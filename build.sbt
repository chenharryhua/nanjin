ThisBuild / scalaVersion := "2.13.12"

ThisBuild / version := "0.17.5-SNAPSHOT"

val catsCoreV   = "2.10.0"
val fs2V        = "3.9.2"
val awsV_1      = "1.12.550"
val awsV_2      = "2.20.150"
val catsEffectV = "3.5.1"
val hadoopV     = "3.3.6"
val monocleV    = "3.2.0"
val confluentV  = "7.5.0"
val kafkaV      = "7.5.0-ce"
val fs2KafkaV   = "3.0.1"
val avroV       = "1.11.2"
val parquetV    = "1.13.1"
val circeV      = "0.14.6"
val kantanV     = "0.7.0"
val slf4jV      = "2.0.9"
val metricsV    = "4.2.19"
val skunkV      = "0.6.0"
val natchezV    = "0.3.3"
val http4sV     = "0.23.23"
val cron4sV     = "0.6.1"
val jacksonV    = "2.15.2"
val protobufV   = "3.24.3"
val sparkV      = "3.4.1"
val refinedV    = "0.11.0"
val nettyV      = "4.1.98.Final"
val chimneyV    = "0.8.0-RC1"
val enumeratumV = "1.7.3"
val drosteV     = "0.9.0"
val log4catsV   = "2.6.0"
val logbackV    = "1.4.11"
val doobieV     = "1.0.0-RC4"
val okioV       = "3.5.0"

lazy val commonSettings = List(
  organization := "com.github.chenharryhua",
  resolvers ++=
    Resolver.sonatypeOssRepos("public") ++
      Resolver.sonatypeOssRepos("releases") :+
      "Confluent Maven Repo".at("https://packages.confluent.io/maven/"),
  addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.2").cross(CrossVersion.full)),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  libraryDependencies ++= List(
    "org.scala-lang" % "scala-reflect"  % scalaVersion.value % Provided,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided
  ),
  evictionErrorLevel := Level.Info,
  versionScheme      := Some("early-semver"),
  scalacOptions ++= List("-Ymacro-annotations", "-Xsource:3", "-Wconf:src=src_managed/.*:silent"),
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
  Test / tpolecatExcludeOptions += org.typelevel.scalacoptions.ScalacOptions.warnNonUnitStatement,
  Test / parallelExecution := false
)

val circeLib = List(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-parser",
  "io.circe" %% "circe-shapes",
  "io.circe" %% "circe-jawn",
  "io.circe" %% "circe-refined"
).map(_ % circeV)

val jacksonLib = List(
  "com.fasterxml.jackson.core"     % "jackson-annotations",
  "com.fasterxml.jackson.core"     % "jackson-core",
  "com.fasterxml.jackson.core"     % "jackson-databind",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8",
  "com.fasterxml.jackson.module"   % "jackson-module-jaxb-annotations",
  "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-base",
  "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-json-provider",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"
).map(_ % jacksonV)

val kantanLib = List(
  "com.nrinaudo" %% "kantan.csv",
  "com.nrinaudo" %% "kantan.csv-java8",
  "com.nrinaudo" %% "kantan.csv-generic",
  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantanV)

val pbLib = List(
  "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.13",
  "com.google.protobuf"                       % "protobuf-java"             % protobufV,
  "com.google.protobuf"                       % "protobuf-java-util"        % protobufV,
  "io.confluent"                              % "kafka-protobuf-serializer" % confluentV,
  "com.squareup.okio" % "okio"     % okioV, // synk by kafka-protobuf-serializer
  "com.squareup.okio" % "okio-jvm" % okioV // synk by kafka-protobuf-serializer
)

val serdeLib = List(
  ("com.sksamuel.avro4s" %% "avro4s-core" % "4.1.1").excludeAll(ExclusionRule(organization = "org.json4s")),
  "org.apache.parquet"                    % "parquet-common"           % parquetV,
  "org.apache.parquet"                    % "parquet-hadoop"           % parquetV,
  "org.apache.parquet"                    % "parquet-avro"             % parquetV,
  "org.apache.avro"                       % "avro"                     % avroV,
  "io.confluent"                          % "kafka-streams-avro-serde" % confluentV
) ++ jacksonLib ++ circeLib ++ pbLib

val fs2Lib = List(
  "co.fs2" %% "fs2-core",
  "co.fs2" %% "fs2-reactive-streams",
  "co.fs2" %% "fs2-io"
).map(_ % fs2V)

val monocleLib = List(
  "dev.optics" %% "monocle-core",
  "dev.optics" %% "monocle-macro"
).map(_ % monocleV)

val testLib = List(
  "org.typelevel" %% "cats-effect-testkit"                    % catsEffectV,
  "org.typelevel" %% "cats-testkit-scalatest"                 % "2.1.5",
  "org.typelevel" %% "discipline-scalatest"                   % "2.2.0",
  "org.typelevel" %% "discipline-munit"                       % "1.0.9",
  "org.typelevel" %% "cats-laws"                              % catsCoreV,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0",
  "org.scalatest" %% "scalatest"                              % "3.2.17",
  "dev.optics" %% "monocle-law"                               % monocleV,
  "com.47deg" %% "scalacheck-toolbox-datetime"                % "0.7.0",
  "org.tpolecat" %% "doobie-postgres"                         % doobieV,
  "org.postgresql"                                            % "postgresql" % "42.6.0", // snyk
  "org.typelevel" %% "algebra-laws"                           % catsCoreV,
  "com.github.pathikrit" %% "better-files"                    % "3.9.2"
).map(_ % Test)

val kafkaLib = List(
  "io.confluent"                              % "kafka-schema-registry-client" % confluentV,
  "io.confluent"                              % "kafka-schema-serializer"      % confluentV,
  "org.apache.kafka"                          % "kafka-streams"                % kafkaV,
  "org.apache.kafka" %% "kafka-streams-scala" % kafkaV,
  ("com.github.fd4s" %% "fs2-kafka"           % fs2KafkaV).exclude("org.apache.kafka", "kafka-clients")
)

val enumLib = List(
  "com.beachape" %% "enumeratum-cats",
  "com.beachape" %% "enumeratum",
  "com.beachape" %% "enumeratum-circe"
).map(_ % enumeratumV)

val drosteLib = List(
  "io.higherkindness" %% "droste-core",
  "io.higherkindness" %% "droste-macros",
  "io.higherkindness" %% "droste-meta"
).map(_ % drosteV)

val catsLib = List(
  "org.typelevel" %% "cats-kernel",
  "org.typelevel" %% "cats-core",
  "org.typelevel" %% "cats-free",
  "org.typelevel" %% "alleycats-core",
  "org.typelevel" %% "algebra"
).map(_ % catsCoreV) ++
  List(
    "org.typelevel" %% "cats-mtl"              % "1.3.1",
    "org.typelevel" %% "kittens"               % "3.0.0",
    "org.typelevel" %% "cats-collections-core" % "0.9.8"
  )

val refinedLib = List(
  "eu.timepit" %% "refined",
  "eu.timepit" %% "refined-cats"
).map(_ % refinedV)

val logLib = List(
  "org.typelevel" %% "log4cats-slf4j" % log4catsV,
  "org.slf4j"                         % "slf4j-api" % slf4jV
)

val jwtLib = List(
  "org.bouncycastle" % "bcpkix-jdk18on" % "1.76",
  "io.jsonwebtoken"  % "jjwt-api"       % "0.11.5",
  "io.jsonwebtoken"  % "jjwt-impl"      % "0.11.5",
  "io.jsonwebtoken"  % "jjwt-jackson"   % "0.11.5"
)

val baseLib = List(
  "org.typelevel" %% "cats-effect"                 % catsEffectV,
  "org.typelevel" %% "cats-time"                   % "0.5.1",
  "org.typelevel" %% "squants"                     % "1.8.3",
  "org.typelevel" %% "case-insensitive"            % "1.4.0",
  "io.scalaland" %% "chimney"                      % chimneyV,
  "io.scalaland" %% "enumz"                        % "1.0.0",
  "com.chuusai" %% "shapeless"                     % "2.3.10",
  "com.github.alonsodomin.cron4s" %% "cron4s-core" % cron4sV
) ++ enumLib ++ drosteLib ++ catsLib ++ refinedLib ++ circeLib ++ monocleLib ++ fs2Lib

lazy val common = (project in file("common"))
  .settings(commonSettings*)
  .settings(name := "nj-common")
  .settings(
    libraryDependencies ++= List(
      "org.apache.commons"               % "commons-lang3" % "3.13.0",
      "io.dropwizard.metrics"            % "metrics-core"  % metricsV % Provided,
      "org.typelevel" %% "log4cats-core" % log4catsV       % Provided
    ) ++ baseLib ++ testLib
  )

lazy val http = (project in file("http"))
  .dependsOn(common)
  .settings(commonSettings*)
  .settings(name := "nj-http")
  .settings(libraryDependencies ++= List(
    "org.http4s" %% "http4s-circe"        % http4sV,
    "org.http4s" %% "http4s-client"       % http4sV,
    "org.http4s" %% "http4s-dsl"          % http4sV,
    "org.tpolecat" %% "natchez-core"      % natchezV,
    "com.fasterxml.jackson.core"          % "jackson-databind" % jacksonV, // snyk
    "org.http4s" %% "http4s-ember-server" % http4sV            % Test,
    "org.http4s" %% "http4s-ember-client" % http4sV            % Test,
    "org.slf4j"                           % "slf4j-reload4j"   % slf4jV % Test
  ) ++ jwtLib ++ logLib ++ testLib)

lazy val aws = (project in file("aws"))
  .dependsOn(common)
  .settings(commonSettings*)
  .settings(name := "nj-aws")
  .settings(libraryDependencies ++= List(
    "software.amazon.awssdk"              % "cloudwatch"       % awsV_2,
    "software.amazon.awssdk"              % "sqs"              % awsV_2,
    "software.amazon.awssdk"              % "ssm"              % awsV_2,
    "software.amazon.awssdk"              % "sns"              % awsV_2,
    "software.amazon.awssdk"              % "ses"              % awsV_2,
    "software.amazon.awssdk"              % "sdk-core"         % awsV_2,
    "com.fasterxml.jackson.core"          % "jackson-databind" % jacksonV, // snyk
    "io.netty"                            % "netty-handler"    % nettyV, // snyk
    "org.http4s" %% "http4s-ember-client" % http4sV,
    "org.http4s" %% "http4s-circe"        % http4sV
  ) ++ logLib ++ testLib)

lazy val datetime = (project in file("datetime"))
  .dependsOn(common)
  .settings(commonSettings*)
  .settings(name := "nj-datetime")
  .settings(
    libraryDependencies ++= List("org.typelevel" %% "cats-parse" % "0.3.10") ++
      testLib
  )

lazy val guard = (project in file("guard"))
  .dependsOn(aws)
  .dependsOn(datetime)
  .settings(commonSettings*)
  .settings(name := "nj-guard")
  .settings(
    libraryDependencies ++= List(
      "com.influxdb"                        % "influxdb-client-java" % "6.10.0",
      "io.dropwizard.metrics"               % "metrics-core"         % metricsV,
      "io.dropwizard.metrics"               % "metrics-jmx"          % metricsV,
      "org.typelevel" %% "vault"            % "3.5.0",
      "com.lihaoyi" %% "scalatags"          % "0.12.0",
      "org.tpolecat" %% "skunk-core"        % skunkV,
      "org.tpolecat" %% "skunk-circe"       % skunkV,
      "org.tpolecat" %% "natchez-core"      % natchezV,
      "org.tpolecat" %% "natchez-noop"      % natchezV,
      "org.http4s" %% "http4s-core"         % http4sV,
      "org.http4s" %% "http4s-dsl"          % http4sV,
      "org.http4s" %% "http4s-ember-server" % http4sV,
      "org.http4s" %% "http4s-scalatags"    % "0.25.2",
      "org.tpolecat" %% "natchez-jaeger"    % natchezV               % Test,
      "org.tpolecat" %% "natchez-log"       % natchezV               % Test,
      "org.slf4j"                           % "slf4j-reload4j"       % slf4jV % Test
    ) ++ logLib ++ testLib
  )

lazy val messages = (project in file("messages"))
  .dependsOn(common)
  .settings(commonSettings*)
  .settings(name := "nj-messages")
  .settings(
    libraryDependencies ++= List(
      "org.apache.commons" % "commons-compress" % "1.24.0", // snyk
      "org.yaml"           % "snakeyaml"        % "2.2", // snyk
      "org.xerial.snappy"  % "snappy-java"      % "1.1.10.3" // snyk
    ) ++ serdeLib ++ kafkaLib.map(_ % Provided) ++ testLib)

lazy val database = (project in file("database"))
  .dependsOn(common)
  .settings(commonSettings*)
  .settings(name := "nj-database")
  .settings(
    libraryDependencies ++= List(
      "org.tpolecat" %% "doobie-core"   % doobieV,
      "org.tpolecat" %% "doobie-hikari" % doobieV,
      "org.tpolecat" %% "doobie-free"   % doobieV,
      "org.tpolecat" %% "skunk-core"    % skunkV,
      ("com.zaxxer"                     % "HikariCP" % "5.0.1").exclude("org.slf4j", "slf4j-api")
    ) ++ testLib
  )

lazy val kafka = (project in file("kafka"))
  .dependsOn(messages)
  .dependsOn(datetime)
  .dependsOn(common)
  .settings(commonSettings*)
  .settings(name := "nj-kafka")
  .settings(libraryDependencies ++= List(
    "ch.qos.logback" % "logback-classic" % logbackV % Test
  ) ++ kafkaLib ++ logLib ++ testLib)

/** hadoop based
  */

val hadoopLib = List(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % hadoopV,
  "org.apache.hadoop" % "hadoop-aws"                   % hadoopV,
  "org.apache.hadoop" % "hadoop-auth"                  % hadoopV,
  "org.apache.hadoop" % "hadoop-annotations"           % hadoopV,
  "org.apache.hadoop" % "hadoop-common"                % hadoopV,
  "org.apache.hadoop" % "hadoop-client"                % hadoopV,
  "org.apache.hadoop" % "hadoop-client-runtime"        % hadoopV,
  "org.apache.hadoop" % "hadoop-hdfs"                  % hadoopV,
  "org.apache.hadoop" % "hadoop-hdfs-client"           % hadoopV,
  "org.slf4j"         % "jcl-over-slf4j"               % slf4jV
).map(
  _.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-reload4j")
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("ch.qos.reload4j", "reload4j")
    .exclude("commons-logging", "commons-logging")
    .exclude("io.netty", "netty") // snyk
)

val sparkLib = List(
  "org.apache.spark" %% "spark-catalyst",
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-streaming",
  "org.apache.spark" %% "spark-streaming-kafka-0-10",
  "org.apache.spark" %% "spark-sql-kafka-0-10",
  "org.apache.spark" %% "spark-avro",
  "org.apache.spark" %% "spark-graphx"
).map(_ % sparkV) ++ List(
  "org.typelevel" %% "frameless-dataset",
  "org.typelevel" %% "frameless-core"
).map(_ % "0.14.1") ++ List(
  "org.apache.avro" % "avro-compiler",
  "org.apache.avro" % "avro-mapred"
).map(_ % avroV)

lazy val pipes = (project in file("pipes"))
  .dependsOn(datetime)
  .dependsOn(messages)
  .settings(commonSettings*)
  .settings(name := "nj-pipes")
  .settings {
    val libs = List(
      "io.circe" %% "circe-jackson212" % "0.14.0",
      "com.amazonaws"                  % "aws-java-sdk-bundle" % awsV_1,
      "org.tukaani"                    % "xz"                  % "1.9",
      "org.eclipse.jetty"              % "jetty-xml"           % "12.0.1", // snyk
      "org.eclipse.jetty"              % "jetty-http"          % "12.0.1", // snyk
      "org.jetbrains.kotlin"           % "kotlin-stdlib"       % "1.9.10", // snyk
      "org.codehaus.jettison"          % "jettison"            % "1.5.4", // snyk
      "io.netty"                       % "netty-handler"       % nettyV, // snyk
      "org.slf4j"                      % "slf4j-jdk14"         % slf4jV % Test
    ) ++ kantanLib ++ logLib ++ testLib ++ hadoopLib
    libraryDependencies ++= libs.map(_.exclude("org.codehaus.jackson", "jackson-mapper-asl")) // snyk
  }

lazy val spark = (project in file("spark"))
  .dependsOn(kafka)
  .dependsOn(pipes)
  .dependsOn(database)
  .settings(commonSettings*)
  .settings(name := "nj-spark")
  .settings(
    libraryDependencies ++= List(
      "org.apache.ivy"                         % "ivy"             % "2.5.2", // snyk
      "com.julianpeeters" %% "avrohugger-core" % "1.5.2"           % Test,
      "ch.qos.logback"                         % "logback-classic" % logbackV % Test
    ) ++ sparkLib.map(_.exclude("commons-logging", "commons-logging")) ++ testLib
  )

lazy val example = (project in file("example"))
  .dependsOn(common)
  .dependsOn(datetime)
  .dependsOn(http)
  .dependsOn(aws)
  .dependsOn(guard)
  .dependsOn(messages)
  .dependsOn(pipes)
  .dependsOn(kafka)
  .dependsOn(database)
  .dependsOn(spark)
  .settings(commonSettings*)
  .settings(name := "nj-example")
  .settings(libraryDependencies ++= List(
    "ch.qos.logback" % "logback-classic" % logbackV % Test
  ) ++ testLib)
  .settings(Compile / PB.targets := List(scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"))

// format: off  
lazy val nanjin =
  (project in file(".")).aggregate(common, datetime, http, aws, guard, messages, pipes, kafka, database, spark)
// format: on

