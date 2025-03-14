ThisBuild / version      := "0.19.1-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.16"

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / Test / parallelExecution := false
ThisBuild / Test / logBuffered       := false

Global / parallelExecution := false

val acyclicV    = "0.3.18"
val avroV       = "1.12.0"
val awsV        = "2.31.0"
val catsCoreV   = "2.13.0"
val catsEffectV = "3.5.7"
val chimneyV    = "1.7.3"
val circeV      = "0.14.10"
val confluentV  = "7.9.0"
val cron4sV     = "0.8.2"
val doobieV     = "1.0.0-RC8"
val drosteV     = "0.10.0"
val enumeratumV = "1.7.5"
val fs2KafkaV   = "3.6.0"
val fs2V        = "3.11.0"
val framelessV  = "0.16.0"
val hadoopV     = "3.4.1"
val http4sV     = "0.23.30"
val jacksonV    = "2.18.3"
val jwtV        = "0.12.6"
val kafkaV      = "7.9.0-ce"
val kantanV     = "0.8.0"
val log4catsV   = "2.7.0"
val logbackV    = "1.5.17"
val metricsV    = "4.2.30"
val monocleV    = "3.3.0"
val natchezV    = "0.3.7"
val nettyV      = "4.1.119.Final"
val parquetV    = "1.15.0"
val postgresV   = "42.7.5"
val refinedV    = "0.11.3"
val skunkV      = "0.6.4"
val slf4jV      = "2.0.17"
val sparkV      = "3.5.5"

lazy val commonSettings = List(
  organization       := "com.github.chenharryhua",
  evictionErrorLevel := Level.Info,
  resolvers ++=
    Resolver.sonatypeOssRepos("public") ++
      Resolver.sonatypeOssRepos("releases") :+
      "Confluent Maven Repo".at("https://packages.confluent.io/maven/"),
  addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.3").cross(CrossVersion.full)),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin(("com.lihaoyi" %% "acyclic" % acyclicV).cross(CrossVersion.full)),
  libraryDependencies ++= List(
    "org.scala-lang" % "scala-reflect"  % scalaVersion.value % Provided,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value % Provided
  ),
  scalacOptions ++= List(
    "-Ymacro-annotations",
    "-Xsource:3",
    "-Xsource-features:case-apply-copy-access",
    "-Wconf:src=src_managed/.*:silent",
    "-Wtostring-interpolated",
    "-Vcyclic",
    "-P:acyclic:warn"
  ),
  Compile / tpolecatExcludeOptions += org.typelevel.scalacoptions.ScalacOptions.warnNonUnitStatement,
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
)

val circeLib = List(
  "io.circe" %% "circe-core",
  "io.circe" %% "circe-generic",
  "io.circe" %% "circe-shapes",
  "io.circe" %% "circe-jawn"
).map(_ % circeV) ++
  List("io.circe" %% "circe-optics" % "0.15.0", "io.circe" %% "circe-refined" % "0.15.1")

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
  ("com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.17").exclude("com.google.protobuf", "protobuf-java"),
  "io.confluent"                               % "kafka-protobuf-serializer" % confluentV
)

val serdeLib = List(
  "com.sksamuel.avro4s" %% "avro4s-core" % "4.1.2",
  "org.apache.parquet"                   % "parquet-common"           % parquetV,
  "org.apache.parquet"                   % "parquet-hadoop"           % parquetV,
  "org.apache.parquet"                   % "parquet-avro"             % parquetV,
  "org.apache.avro"                      % "avro"                     % avroV,
  "io.confluent"                         % "kafka-streams-avro-serde" % confluentV
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
  "org.typelevel" %% "discipline-scalatest"                   % "2.3.0",
  "org.typelevel" %% "discipline-munit"                       % "2.0.0",
  "org.typelevel" %% "cats-laws"                              % catsCoreV,
  "org.typelevel" %% "algebra-laws"                           % catsCoreV,
  "com.github.alexarchambault" %% "scalacheck-shapeless_1.15" % "1.3.0",
  "org.scalatest" %% "scalatest"                              % "3.2.19",
  "dev.optics" %% "monocle-law"                               % monocleV,
  "com.47deg" %% "scalacheck-toolbox-datetime"                % "0.7.0",
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
    "org.typelevel" %% "cats-mtl"              % "1.5.0",
    "org.typelevel" %% "kittens"               % "3.5.0",
    "org.typelevel" %% "cats-collections-core" % "0.9.9"
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
  "org.bouncycastle"           % "bcpkix-jdk18on"   % "1.80",
  "io.jsonwebtoken"            % "jjwt-api"         % jwtV,
  "io.jsonwebtoken"            % "jjwt-impl"        % jwtV,
  "io.jsonwebtoken"            % "jjwt-jackson"     % jwtV,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonV // snyk
)

val baseLib = List(
  "org.typelevel" %% "cats-effect"                 % catsEffectV,
  "org.typelevel" %% "cats-time"                   % "0.5.1",
  "org.typelevel" %% "squants"                     % "1.8.3",
  "org.typelevel" %% "case-insensitive"            % "1.4.2",
  "io.scalaland" %% "chimney"                      % chimneyV,
  "io.scalaland" %% "enumz"                        % "1.2.0",
  "com.chuusai" %% "shapeless"                     % "2.3.13",
  "com.github.alonsodomin.cron4s" %% "cron4s-core" % cron4sV,
  "com.indoorvivants" %% "scala-uri"               % "4.2.0",
  ("com.lihaoyi" %% "acyclic"                      % acyclicV).cross(CrossVersion.full) % "provided"
) ++ enumLib ++ drosteLib ++ catsLib ++ refinedLib ++ circeLib ++ monocleLib ++ fs2Lib

lazy val common = (project in file("common"))
  .settings(commonSettings *)
  .settings(name := "nj-common")
  .settings(
    libraryDependencies ++= List(
      "org.apache.commons"    % "commons-lang3" % "3.17.0",
      "io.dropwizard.metrics" % "metrics-core"  % metricsV % Provided
    ) ++ baseLib ++ testLib
  )

lazy val http = (project in file("http"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-http")
  .settings(
    libraryDependencies ++= List(
      "org.http4s" %% "http4s-circe"        % http4sV,
      "org.http4s" %% "http4s-client"       % http4sV,
      "org.http4s" %% "http4s-dsl"          % http4sV,
      "org.tpolecat" %% "natchez-core"      % natchezV,
      "org.http4s" %% "http4s-ember-server" % http4sV          % Test,
      "org.http4s" %% "http4s-ember-client" % http4sV          % Test,
      "org.tpolecat" %% "natchez-log"       % natchezV         % Test,
      "org.slf4j"                           % "slf4j-reload4j" % slf4jV % Test
    ) ++ jwtLib ++ testLib)

lazy val aws = (project in file("aws"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-aws")
  .settings(libraryDependencies ++= List(
    "software.amazon.awssdk"              % "cloudwatch" % awsV,
    "software.amazon.awssdk"              % "sqs"        % awsV,
    "software.amazon.awssdk"              % "ssm"        % awsV,
    "software.amazon.awssdk"              % "sns"        % awsV,
    "software.amazon.awssdk"              % "ses"        % awsV,
    "software.amazon.awssdk"              % "sdk-core"   % awsV,
    "org.http4s" %% "http4s-ember-client" % http4sV,
    "org.http4s" %% "http4s-circe"        % http4sV
  ) ++ logLib ++ testLib)

lazy val datetime = (project in file("datetime"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-datetime")
  .settings(
    libraryDependencies ++= List("org.typelevel" %% "cats-parse" % "1.1.0") ++
      testLib
  )

lazy val guard = (project in file("guard"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-guard")
  .settings(
    libraryDependencies ++= List(
      "commons-codec"                       % "commons-codec"        % "1.18.0",
      "org.apache.commons"                  % "commons-collections4" % "4.4",
      "io.dropwizard.metrics"               % "metrics-core"         % metricsV,
      "io.dropwizard.metrics"               % "metrics-jmx"          % metricsV,
      "com.github.ben-manes.caffeine"       % "caffeine"             % "3.1.8",
      "com.lihaoyi" %% "scalatags"          % "0.13.1",
      "org.http4s" %% "http4s-core"         % http4sV,
      "org.http4s" %% "http4s-dsl"          % http4sV,
      "org.http4s" %% "http4s-ember-server" % http4sV,
      "org.http4s" %% "http4s-circe"        % http4sV,
      "org.http4s" %% "http4s-scalatags"    % "0.25.2",
      "org.http4s" %% "http4s-ember-client" % http4sV                % Test
    ) ++ testLib
  )

lazy val observer_logging = (project in file("observers/logging"))
  .dependsOn(guard)
  .settings(commonSettings *)
  .settings(name := "nj-observer-logging")
  .settings(
    libraryDependencies ++=
      List("org.slf4j" % "slf4j-reload4j" % slf4jV % Test) ++ logLib ++ testLib
  )

lazy val observer_aws = (project in file("observers/aws"))
  .dependsOn(guard)
  .dependsOn(aws)
  .settings(commonSettings *)
  .settings(name := "nj-observer-aws")
  .settings(
    libraryDependencies ++= testLib
  )

lazy val observer_kafka = (project in file("observers/kafka"))
  .dependsOn(guard)
  .dependsOn(kafka)
  .settings(commonSettings *)
  .settings(name := "nj-observer-kafka")
  .settings(
    libraryDependencies ++= testLib
  )

lazy val observer_database = (project in file("observers/database"))
  .dependsOn(guard)
  .dependsOn(database)
  .settings(commonSettings *)
  .settings(name := "nj-observer-database")
  .settings(
    libraryDependencies ++= List(
      "org.tpolecat" %% "skunk-circe" % skunkV
    ) ++ testLib
  )

lazy val messages = (project in file("messages"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-messages")
  .settings(
    libraryDependencies ++=
      List("org.typelevel" %% "frameless-core" % framelessV) ++
        serdeLib ++ kafkaLib.map(_ % Provided) ++ testLib)

lazy val database = (project in file("database"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-database")
  .settings(
    libraryDependencies ++= List(
      "org.tpolecat" %% "doobie-core"   % doobieV,
      "org.tpolecat" %% "doobie-hikari" % doobieV,
      "org.tpolecat" %% "doobie-free"   % doobieV,
      "org.tpolecat" %% "skunk-core"    % skunkV,
      ("com.zaxxer"                     % "HikariCP"   % "6.2.1").exclude("org.slf4j", "slf4j-api"),
      "org.postgresql"                  % "postgresql" % postgresV % Test
    ) ++ testLib
  )

lazy val kafka = (project in file("kafka"))
  .dependsOn(common)
  .dependsOn(messages)
  .dependsOn(datetime)
  .settings(commonSettings *)
  .settings(name := "nj-kafka")
  .settings(libraryDependencies ++= List(
    "ch.qos.logback" % "logback-classic" % logbackV % Test
  ) ++ kafkaLib ++ testLib)

/** hadoop based
  */

val hadoopLib = List(
  "org.apache.hadoop"  % "hadoop-mapreduce-client-core" % hadoopV,
  "org.apache.hadoop"  % "hadoop-aws"                   % hadoopV,
  "org.apache.hadoop"  % "hadoop-auth"                  % hadoopV,
  "org.apache.hadoop"  % "hadoop-annotations"           % hadoopV,
  "org.apache.hadoop"  % "hadoop-common"                % hadoopV,
  "org.apache.hadoop"  % "hadoop-client"                % hadoopV,
  "org.apache.hadoop"  % "hadoop-client-runtime"        % hadoopV,
  "org.apache.hadoop"  % "hadoop-hdfs"                  % hadoopV,
  "org.apache.hadoop"  % "hadoop-hdfs-client"           % hadoopV,
  "org.slf4j"          % "jcl-over-slf4j"               % slf4jV,
  "org.eclipse.jetty"  % "jetty-server"                 % "12.0.17", // snyk
  "io.netty"           % "netty-all"                    % nettyV, // snyk
  "com.nimbusds"       % "nimbus-jose-jwt"              % "10.0.2", // snyk
  "dnsjava"            % "dnsjava"                      % "3.6.3", // snyk
  "com.google.guava"   % "guava"                        % "33.4.0-jre", // snyk
  "org.apache.commons" % "commons-configuration2"       % "2.11.0" // snyk
).map(
  _.exclude("log4j", "log4j")
    .exclude("org.slf4j", "slf4j-reload4j")
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("ch.qos.reload4j", "reload4j")
    .exclude("commons-logging", "commons-logging")
    .exclude("io.netty", "netty") // snyk
)

lazy val pipes = (project in file("pipes"))
  .dependsOn(common)
  .dependsOn(datetime)
  .settings(commonSettings *)
  .settings(name := "nj-pipes")
  .settings {
    val libs = List(
      "io.circe" %% "circe-jackson210" % "0.14.2",
      "software.amazon.awssdk"         % "bundle"    % awsV,
      "org.tukaani"                    % "xz"        % "1.10",
      "org.apache.zookeeper"           % "zookeeper" % "3.9.3", // snyk
      "org.typelevel" %% "jawn-fs2"    % "2.4.0"     % Test
    ) ++ kantanLib ++ serdeLib ++ hadoopLib ++ testLib
    libraryDependencies ++= libs.map(_.exclude("org.codehaus.jackson", "jackson-mapper-asl")) // snyk
  }

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
).map(_ % framelessV) ++ List(
  "org.apache.avro" % "avro-compiler",
  "org.apache.avro" % "avro-mapred"
).map(_ % avroV)

lazy val spark = (project in file("spark"))
  .dependsOn(common)
  .dependsOn(datetime)
  .dependsOn(messages)
  .dependsOn(kafka)
  .dependsOn(pipes)
  .dependsOn(database)
  .settings(commonSettings *)
  .settings(name := "nj-spark")
  .settings(
    libraryDependencies ++= List(
      "org.apache.ivy"                         % "ivy"             % "2.5.3", // snyk
      "com.julianpeeters" %% "avrohugger-core" % "2.12.0"          % Test,
      "ch.qos.logback"                         % "logback-classic" % logbackV  % Test,
      "org.postgresql"                         % "postgresql"      % postgresV % Test
    ) ++ sparkLib.map(_.exclude("commons-logging", "commons-logging")) ++ testLib
  )
  .settings(dependencyOverrides += "org.json4s" %% "json4s-native" % "3.6.12")

lazy val example = (project in file("example"))
  .dependsOn(common)
  .dependsOn(datetime)
  .dependsOn(http)
  .dependsOn(aws)
  .dependsOn(messages)
  .dependsOn(pipes)
  .dependsOn(kafka)
  .dependsOn(database)
  .dependsOn(spark)
  .dependsOn(guard)
  .dependsOn(observer_logging)
  .dependsOn(observer_aws)
  .dependsOn(observer_database)
  .dependsOn(observer_kafka)
  .settings(commonSettings *)
  .settings(name := "nj-example")
  .settings(libraryDependencies ++= List(
    "ch.qos.logback" % "logback-classic" % logbackV % Test
  ) ++ testLib)
  .settings(Test / PB.targets := Seq(
    scalapb.gen() -> (Test / sourceManaged).value / "scalapb"
  ))
  .settings(dependencyOverrides += "org.json4s" %% "json4s-native" % "3.6.12")

lazy val nanjin =
  (project in file(".")).aggregate(
    common,
    datetime,
    http,
    aws,
    messages,
    pipes,
    kafka,
    database,
    spark,
    guard,
    observer_aws,
    observer_database,
    observer_kafka,
    observer_logging
  )
