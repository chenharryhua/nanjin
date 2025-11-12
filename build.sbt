ThisBuild / version      := "0.19.19-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.17"

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / Test / parallelExecution := false
ThisBuild / Test / logBuffered       := false

Global / parallelExecution := false

val acyclicV = "0.3.19"
val avroV = "1.12.1"
val avro4sV = "4.1.2"
val awsV = "2.38.2"
val caffeineV = "3.2.3"
val catsCoreV = "2.13.0"
val catsEffectV = "3.6.3"
val chimneyV = "1.8.2"
val circeV = "0.14.15"
val confluentV = "8.1.0"
val cron4sV = "0.8.2"
val doobieV = "1.0.0-RC11"
val drosteV = "0.10.0"
val enumeratumV = "1.9.0"
val fs2KafkaV = "3.9.1"
val fs2V = "3.12.2"
val hadoopV = "3.4.2"
val http4sV = "0.23.33"
val jacksonV = "2.20.1"
val jwtV = "0.13.0"
val kafkaV = "8.1.0-ce"
val kantanV = "0.8.0"
val log4catsV = "2.7.1"
val logbackV = "1.5.21"
val metricsV = "4.2.37"
val monocleV = "3.3.0"
val natchezV = "0.3.8"
val nettyV = "4.2.7.Final"
val parquetV = "1.16.0"
val postgresV = "42.7.8"
val refinedV = "0.11.3"
val shapelessV = "2.3.13"
val skunkV = "0.6.4"
val slf4jV = "2.0.17"
val sparkV = "4.0.1"

lazy val commonSettings = List(
  organization       := "com.github.chenharryhua",
  evictionErrorLevel := Level.Info,
  resolvers += "Confluent Maven Repo".at("https://packages.confluent.io/maven/"),
  addCompilerPlugin(("org.typelevel" %% "kind-projector" % "0.13.4").cross(CrossVersion.full)),
  addCompilerPlugin("com.olegpy" %% "better-monadic-for" % "0.3.1"),
  addCompilerPlugin(("com.lihaoyi" %% "acyclic" % acyclicV).cross(CrossVersion.full)),
  libraryDependencies ++= List(
    "org.scala-lang"            % "scala-reflect"                    % scalaVersion.value % Runtime,
    "org.scala-lang"            % "scala-compiler"                   % scalaVersion.value % Runtime,
    ("com.lihaoyi" %% "acyclic" % acyclicV).cross(CrossVersion.full) % Runtime
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
  Test / tpolecatExcludeOptions ++=
    org.typelevel.scalacoptions.ScalacOptions.lintOptions
      .filterNot(_.option.startsWith("-Xlint:kind-projector")) +
      org.typelevel.scalacoptions.ScalacOptions.warnNonUnitStatement,
  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat
)

val testLib = List(
  "org.typelevel" %% "cats-effect-testing-scalatest"          % "1.7.0",
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

val enumLib = List(
  "com.beachape" %% "enumeratum"       % enumeratumV,
  "com.beachape" %% "enumeratum-cats"  % enumeratumV,
  "com.beachape" %% "enumeratum-circe" % enumeratumV
)

val refinedLib = List(
  "eu.timepit" %% "refined",
  "eu.timepit" %% "refined-cats"
).map(_ % refinedV)

lazy val common = (project in file("common"))
  .settings(commonSettings *)
  .settings(name := "nj-common")
  .settings(
    libraryDependencies ++= List(
      "io.estatico" %% "newtype"                       % "0.4.4",
      "com.github.alonsodomin.cron4s" %% "cron4s-core" % cron4sV,
      "org.typelevel" %% "cats-time"                   % "0.6.0",
      "org.typelevel" %% "squants"                     % "1.8.3",
      "org.typelevel" %% "cats-kernel"                 % catsCoreV,
      "org.typelevel" %% "cats-core"                   % catsCoreV,
      "org.typelevel" %% "kittens"                     % "3.5.0",
      "io.scalaland" %% "chimney"                      % chimneyV,
      "io.scalaland" %% "enumz"                        % "1.2.0",
      "com.chuusai" %% "shapeless"                     % shapelessV,
      "io.higherkindness" %% "droste-core"             % drosteV,
      "co.fs2" %% "fs2-core"                           % fs2V,
      "io.circe" %% "circe-core"                       % circeV,
      "io.circe" %% "circe-generic"                    % circeV,
      "io.circe" %% "circe-refined"                    % "0.15.1",
      "dev.optics" %% "monocle-macro"                  % monocleV,
      "org.apache.commons"                             % "commons-lang3" % "3.19.0",
      "io.circe" %% "circe-jawn"                       % circeV          % Test
    ) ++ enumLib ++ refinedLib ++ testLib
  )

lazy val http = (project in file("http"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-http")
  .settings(
    libraryDependencies ++= List(
      "org.http4s" %% "http4s-circe"        % http4sV,
      "org.http4s" %% "http4s-client"       % http4sV,
      "org.tpolecat" %% "natchez-core"      % natchezV,
      "org.bouncycastle"                    % "bcpkix-jdk18on" % "1.82",
      "io.jsonwebtoken"                     % "jjwt-api"       % jwtV,
      "co.fs2" %% "fs2-io"                  % fs2V, // snyk - http4s
      "org.http4s" %% "http4s-dsl"          % http4sV          % Test,
      "org.http4s" %% "http4s-ember-server" % http4sV          % Test,
      "org.http4s" %% "http4s-ember-client" % http4sV          % Test,
      "org.tpolecat" %% "natchez-log"       % natchezV         % Test,
      "org.slf4j"                           % "slf4j-reload4j" % slf4jV % Test
    ) ++ testLib)

val awsLib = List(
  "software.amazon.awssdk" % "cloudwatch",
  "software.amazon.awssdk" % "secretsmanager",
  "software.amazon.awssdk" % "sqs",
  "software.amazon.awssdk" % "ssm",
  "software.amazon.awssdk" % "sns",
  "software.amazon.awssdk" % "ses"
).map(_ % awsV)

lazy val aws = (project in file("aws"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-aws")
  .settings(libraryDependencies ++= List(
    "org.typelevel" %% "log4cats-slf4j"   % log4catsV,
    "org.http4s" %% "http4s-ember-client" % http4sV,
    "org.http4s" %% "http4s-circe"        % http4sV,
    "co.fs2" %% "fs2-io"                  % fs2V // snyk
  ) ++ awsLib ++ testLib)

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
      "org.apache.commons"                  % "commons-collections4" % "4.5.0",
      "io.dropwizard.metrics"               % "metrics-core"         % metricsV,
      "io.dropwizard.metrics"               % "metrics-jmx"          % metricsV,
      "com.github.ben-manes.caffeine"       % "caffeine"             % caffeineV,
      "io.github.timwspence" %% "cats-stm"  % "0.13.5",
      "org.typelevel" %% "log4cats-slf4j"   % log4catsV,
      "io.circe" %% "circe-optics"          % "0.15.1",
      "org.http4s" %% "http4s-core"         % http4sV,
      "org.http4s" %% "http4s-dsl"          % http4sV,
      "org.http4s" %% "http4s-ember-server" % http4sV,
      "org.http4s" %% "http4s-circe"        % http4sV,
      "org.http4s" %% "http4s-scalatags"    % "0.25.2",
      "co.fs2" %% "fs2-io"                  % fs2V, // snyk - http4s
      "org.http4s" %% "http4s-ember-client" % http4sV                % Test,
      "ch.qos.logback"                      % "logback-classic"      % logbackV % Test
    ) ++ testLib
  )
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](
      ThisBuild / version,
      scalaVersion,
      git.gitHeadCommit,
      git.gitCurrentBranch
    ),
    buildInfoPackage := "com.github.chenharryhua.nanjin.guard.config",
    buildInfoOptions += BuildInfoOption.ToJson
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
      "com.zaxxer"                      % "HikariCP"        % "7.0.2",
      "co.fs2" %% "fs2-io"              % fs2V, // snyk - http4s
      "org.postgresql"                  % "postgresql"      % postgresV % Test,
      "ch.qos.logback"                  % "logback-classic" % logbackV  % Test
    ) ++ testLib
  )

val jacksonLib = List(
  "com.fasterxml.jackson.core"     % "jackson-core",
  "com.fasterxml.jackson.core"     % "jackson-databind",
  "com.fasterxml.jackson.datatype" % "jackson-datatype-jdk8",
  "com.fasterxml.jackson.module"   % "jackson-module-jaxb-annotations",
  "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-base",
  "com.fasterxml.jackson.jaxrs"    % "jackson-jaxrs-json-provider",
  "com.fasterxml.jackson.module" %% "jackson-module-scala"
).map(_ % jacksonV)

lazy val messages =
  (project in file("messages"))
    .dependsOn(common)
    .settings(commonSettings *)
    .settings(name := "nj-messages")
    .settings(
      libraryDependencies ++=
        List(
          "io.circe" %% "circe-optics"                % "0.15.1",
          "io.circe" %% "circe-jawn"                  % circeV,
          "org.apache.kafka" %% "kafka-streams-scala" % kafkaV,
          "com.github.fd4s" %% "fs2-kafka"            % fs2KafkaV,
          "com.sksamuel.avro4s" %% "avro4s-core"      % avro4sV,
          "org.apache.avro"                           % "avro"                         % avroV,
          "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.20",
          "io.confluent"                              % "kafka-protobuf-serializer"    % confluentV,
          "io.confluent"                              % "kafka-json-schema-serializer" % confluentV,
          "io.confluent"                              % "kafka-streams-avro-serde"     % confluentV,
          "com.google.protobuf"                       % "protobuf-java"                % "4.33.0", // snyk
          "org.jetbrains.kotlin"                      % "kotlin-stdlib"                % "2.2.21", // snyk
          "io.circe" %% "circe-shapes"                % circeV                         % Test
        ) ++ jacksonLib ++ testLib)
    .settings(Compile / PB.targets := List(scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"))

lazy val kafka = (project in file("kafka"))
  .dependsOn(messages)
  .dependsOn(datetime)
  .settings(commonSettings *)
  .settings(name := "nj-kafka")
  .settings(
    libraryDependencies ++= List(
      "io.confluent"                              % "kafka-schema-registry-client" % confluentV,
      "io.confluent"                              % "kafka-schema-serializer"      % confluentV,
      "org.apache.kafka"                          % "kafka-streams"                % kafkaV,
      "org.apache.kafka" %% "kafka-streams-scala" % kafkaV,
      ("com.github.fd4s" %% "fs2-kafka"           % fs2KafkaV).exclude("org.apache.kafka", "kafka-clients"),
      "ch.qos.logback"                            % "logback-classic"              % logbackV % Test
    ) ++ jacksonLib ++ testLib)

/** hadoop based
  */

val hadoopLib = List(
  "org.apache.hadoop" % "hadoop-mapreduce-client-core",
  "org.apache.hadoop" % "hadoop-aws",
  "org.apache.hadoop" % "hadoop-auth",
  "org.apache.hadoop" % "hadoop-annotations",
  "org.apache.hadoop" % "hadoop-common",
  "org.apache.hadoop" % "hadoop-client",
  "org.apache.hadoop" % "hadoop-client-runtime",
  "org.apache.hadoop" % "hadoop-hdfs",
  "org.apache.hadoop" % "hadoop-hdfs-client"
).map(_ % hadoopV)

val kantanLib = List(
  "com.nrinaudo" %% "kantan.csv",
  "com.nrinaudo" %% "kantan.csv-java8",
  "com.nrinaudo" %% "kantan.csv-generic",
  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantanV)

lazy val pipes = (project in file("pipes"))
  .dependsOn(messages)
  .dependsOn(datetime)
  .settings(commonSettings *)
  .settings(name := "nj-pipes")
  .settings {
    val libs = List(
      "co.fs2" %% "fs2-io"               % fs2V,
      "com.nrinaudo" %% "kantan.csv"     % kantanV,
      "com.indoorvivants" %% "scala-uri" % "4.2.0",
      "software.amazon.awssdk"           % "bundle"          % awsV,
      "org.apache.parquet"               % "parquet-common"  % parquetV,
      "org.apache.parquet"               % "parquet-hadoop"  % parquetV,
      "org.apache.parquet"               % "parquet-avro"    % parquetV,
      "org.apache.avro"                  % "avro"            % avroV,
      "org.tukaani"                      % "xz"              % "1.10",
      "org.eclipse.jetty"                % "jetty-server"    % "12.1.3", // snyk
      "io.netty"                         % "netty-all"       % nettyV, // snyk
      "com.nimbusds"                     % "nimbus-jose-jwt" % "10.6", // snyk
      "org.apache.zookeeper"             % "zookeeper"       % "3.9.4", // snyk
      "org.typelevel" %% "jawn-fs2"      % "2.4.0"           % Test
    ) ++ jacksonLib ++ hadoopLib ++ kantanLib
    libraryDependencies ++= libs ++ testLib
  }

val sparkLib = List(
  "org.apache.spark" %% "spark-catalyst",
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
  "org.apache.spark" %% "spark-avro"
).map(_ % sparkV)

lazy val spark = (project in file("spark"))
  .dependsOn(kafka)
  .dependsOn(pipes)
  .dependsOn(database)
  .settings(commonSettings *)
  .settings(name := "nj-spark")
  .settings {
    val libs = List(
      "org.apache.avro"                        % "avro-mapred"     % avroV,
      "org.apache.ivy"                         % "ivy"             % "2.5.3", // snyk
      "com.julianpeeters" %% "avrohugger-core" % "2.15.0"          % Test,
      "io.circe" %% "circe-shapes"             % circeV            % Test,
      "ch.qos.logback"                         % "logback-classic" % logbackV  % Test,
      "org.postgresql"                         % "postgresql"      % postgresV % Test
    ) ++ jacksonLib ++ sparkLib
    libraryDependencies ++= libs ++ testLib
  }

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
  .dependsOn(observer_aws)
  .dependsOn(observer_database)
  .dependsOn(observer_kafka)
  .settings(commonSettings *)
  .settings(name := "nj-example")
  .settings(libraryDependencies ++= List(
    "ch.qos.logback" % "logback-classic" % logbackV % Test
  ) ++ testLib)
  .settings(Test / PB.targets := List(
    scalapb.gen() -> (Test / sourceManaged).value / "scalapb"
  ))

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
    observer_kafka
  )
