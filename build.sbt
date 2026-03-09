ThisBuild / version      := "0.20.9-SNAPSHOT"
ThisBuild / scalaVersion := "3.8.2"

ThisBuild / versionScheme := Some("early-semver")

ThisBuild / Test / parallelExecution := false
ThisBuild / Test / logBuffered       := false

Global / parallelExecution := false

// ==========================
// Versions
// ==========================
val acyclicV = "0.3.20"
val avroV = "1.12.1"
val avro4sV = "5.0.15"
val awsV = "2.42.8"
val caffeineV = "3.2.3"
val catsCoreV = "2.13.0"
val catsEffectV = "3.7.0"
val chimneyV = "1.9.0"
val circeV = "0.14.15"
val confluentV = "8.2.0"
val kafkaV = "8.2.0-ce"
val cron4sV = "0.8.2"
val doobieV = "1.0.0-RC12"
val drosteV = "0.10.0"
val enumeratumV = "1.9.6"
val fs2KafkaV = "3.9.1"
val fs2V = "3.12.2"
val hadoopV = "3.4.3"
val http4sV = "0.23.33"
val jacksonV = "2.21.1"
val kantanV = "0.8.0"
val log4catsV = "2.7.1"
val logbackV = "1.5.32"
val metricsV = "4.2.38"
val monocleV = "3.3.0"
val natchezV = "0.3.9"
val nettyV = "4.2.10.Final"
val parquetV = "1.17.0"
val postgresV = "42.7.10"
val refinedV = "0.11.3"
val shapelessV = "3.5.0"
val skunkV = "0.6.5"
val slf4jV = "2.0.17"
val sparkV = "4.1.1"

lazy val commonSettings = List(
  organization       := "com.github.chenharryhua",
  evictionErrorLevel := Level.Info,
  resolvers += "Confluent Maven Repo".at("https://packages.confluent.io/maven/"),
  dependencyUpdatesFilter := { _.organization != "org.scala-lang" },
  scalacOptions ++= List(
    "-Wconf:src=src_managed/.*:silent",
    "-Wtostring-interpolated",
    "-Yretain-trees",
    "-Wconf:msg=Implicit parameters should be provided with a `using` clause:s"
  ),

  Test / tpolecatExcludeOptions ++=
    org.typelevel.scalacoptions.ScalacOptions.lintOptions +
      org.typelevel.scalacoptions.ScalacOptions.warnNonUnitStatement,

  Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,

  // scalafix
  semanticdbEnabled           := true,
  semanticdbVersion           := scalafixSemanticdb.revision,
  Compile / scalafixOnCompile := true,
  Test / scalafixOnCompile    := false,
  Compile / scalafixConfig    := Option((ThisBuild / baseDirectory).value / ".scalafix.conf"),
  Test / scalafixConfig       := Option((ThisBuild / baseDirectory).value / ".scalafix-test.conf")
)

val testLib = List(
  "org.typelevel" %% "cats-effect-testing-scalatest" % "1.7.0",
  "org.typelevel" %% "cats-effect-testkit"           % catsEffectV,
  "org.typelevel" %% "cats-testkit-scalatest"        % "2.1.5",
  "org.typelevel" %% "discipline-scalatest"          % "2.3.0",
  "org.typelevel" %% "discipline-munit"              % "2.0.0",
  "org.typelevel" %% "cats-laws"                     % catsCoreV,
  "org.typelevel" %% "algebra-laws"                  % catsCoreV,
  "org.typelevel" %% "munit-cats-effect"             % "2.1.0",
  "org.scalatest" %% "scalatest"                     % "3.2.19",
  "dev.optics" %% "monocle-law"                      % monocleV,
  "com.47deg" %% "scalacheck-toolbox-datetime"       % "0.7.0",
  "com.github.pathikrit" %% "better-files"           % "3.9.2",
  "io.circe" %% "circe-jawn"                         % circeV
).map(_ % Test)

val enumLib = List(
  "com.beachape" %% "enumeratum",
  "com.beachape" %% "enumeratum-cats",
  "com.beachape" %% "enumeratum-circe"
).map(_ % enumeratumV)

// ==========================
// Common
// ==========================
lazy val common = (project in file("common"))
  .settings(commonSettings *)
  .settings(name := "nj-common")
  .settings(
    libraryDependencies ++= List(
      "com.github.alonsodomin.cron4s" %% "cron4s-core" % cron4sV,
      "org.typelevel" %% "cats-time"                   % "0.6.0",
      "org.typelevel" %% "squants"                     % "1.8.3",
      "org.typelevel" %% "cats-kernel"                 % catsCoreV,
      "org.typelevel" %% "cats-core"                   % catsCoreV,
      "org.typelevel" %% "kittens"                     % "3.5.0",
      "io.scalaland" %% "chimney"                      % chimneyV,
      "io.higherkindness" %% "droste-core"             % drosteV,
      "co.fs2" %% "fs2-core"                           % fs2V,
      "io.circe" %% "circe-core"                       % circeV,
      "io.circe" %% "circe-generic"                    % circeV,
      "dev.optics" %% "monocle-macro"                  % monocleV,
      "io.github.iltotore" %% "iron"                   % "3.3.0",
      "io.github.iltotore" %% "iron-circe"             % "3.3.0",
      // java
      "org.apache.commons" % "commons-lang3" % "3.20.0"
    ) ++ enumLib ++ testLib
  )

// ==========================
// Http
// ==========================
lazy val http = (project in file("http"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-http")
  .settings(
    libraryDependencies ++= List(
      "org.http4s" %% "http4s-circe"        % http4sV,
      "org.http4s" %% "http4s-client"       % http4sV,
      "org.tpolecat" %% "natchez-core"      % natchezV,
      "org.http4s" %% "http4s-dsl"          % http4sV  % Test,
      "org.http4s" %% "http4s-ember-server" % http4sV  % Test,
      "org.http4s" %% "http4s-ember-client" % http4sV  % Test,
      "org.tpolecat" %% "natchez-log"       % natchezV % Test,
      // java
      "org.slf4j" % "slf4j-reload4j" % slf4jV % Test
    ) ++ testLib)

// ==========================
// Aws
// ==========================
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
  .settings(
    libraryDependencies ++= List(
      "org.typelevel" %% "log4cats-slf4j"   % log4catsV,
      "org.http4s" %% "http4s-ember-client" % http4sV,
      "org.http4s" %% "http4s-circe"        % http4sV
    ) ++ awsLib ++ testLib
  )

// ==========================
// Date-time
// ==========================
lazy val datetime = (project in file("datetime"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-datetime")
  .settings(
    libraryDependencies ++= List("org.typelevel" %% "cats-parse" % "1.1.0") ++
      testLib
  )

// ==========================
// Guard
// ==========================

lazy val frontend = project.in(file("frontend"))
  .enablePlugins(ScalaJSPlugin)
  .settings(name := "nj-frontend")
  .settings(
    scalaJSUseMainModuleInitializer := true,
    libraryDependencies ++= List(
      "io.circe" %%% "circe-core"      % circeV,
      "io.circe" %%% "circe-generic"   % circeV,
      "io.circe" %%% "circe-jawn"      % circeV,
      "org.scala-js" %%% "scalajs-dom" % "2.8.1",
      "com.raquo" %%% "laminar"        % "17.0.0"
    )
  )

lazy val guard = (project in file("guard"))
  .dependsOn(common)
  .settings(commonSettings *)
  .settings(name := "nj-guard")
  .settings(
    libraryDependencies ++= List(
      "io.github.timwspence" %% "cats-stm"  % "0.13.5",
      "org.typelevel" %% "log4cats-slf4j"   % log4catsV,
      "io.circe" %% "circe-optics"          % "0.15.1",
      "org.http4s" %% "http4s-core"         % http4sV,
      "org.http4s" %% "http4s-dsl"          % http4sV,
      "org.http4s" %% "http4s-ember-server" % http4sV,
      "org.http4s" %% "http4s-circe"        % http4sV,
      "org.http4s" %% "http4s-scalatags"    % "0.25.2",
      "org.http4s" %% "http4s-ember-client" % http4sV % Test,
      // java
      "org.apache.commons"            % "commons-collections4" % "4.5.0",
      "io.dropwizard.metrics"         % "metrics-core"         % metricsV,
      "com.github.ben-manes.caffeine" % "caffeine"             % caffeineV,
      "ch.qos.logback"                % "logback-classic"      % logbackV % Test
    ) ++ testLib
  )
  .settings {
    Compile / resourceGenerators += Def.task {
      val js = (frontend / Compile / fullOptJS).value
      val map = (frontend / Compile / fullOptJS).value.data.getParentFile / (js.data.getName + ".map")
      val targetDir = (Compile / resourceManaged).value / "dashboard"
      val jsOut = targetDir / "nj-frontend.js"
      val mapOut = targetDir / "nj-frontend-opt.js.map"
      IO.createDirectory(targetDir)
      IO.copyFile(js.data, jsOut)
      if (map.exists()) IO.copyFile(map, mapOut)
      List(jsOut, mapOut).filter(_.exists())
    }.taskValue
  }
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

// ==========================
// Observers
// ==========================
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

// ==========================
// Database
// ==========================
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
      // java
      "com.zaxxer"     % "HikariCP"        % "7.0.2",
      "org.postgresql" % "postgresql"      % postgresV % Test,
      "ch.qos.logback" % "logback-classic" % logbackV  % Test
    ) ++ testLib
  )

// ==========================
// Messages
// ==========================
val jacksonLib = List(
  "com.fasterxml.jackson.core" % "jackson-core",
  "com.fasterxml.jackson.core" % "jackson-databind",
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
          "com.github.fd4s" %% "fs2-kafka"            % fs2KafkaV,
          "com.sksamuel.avro4s" %% "avro4s-core"      % avro4sV,
          "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.20",
          //  "io.circe" %% "circe-shapes"                % circeV % Test,
          // java
          "org.apache.avro" % "avro"                         % avroV,
          "io.confluent"    % "kafka-protobuf-serializer"    % confluentV,
          "io.confluent"    % "kafka-json-schema-serializer" % confluentV,
          "io.confluent"    % "kafka-streams-avro-serde"     % confluentV
        ) ++ jacksonLib ++ testLib
    )
    .settings(Compile / PB.targets := List(scalapb.gen() -> (Compile / sourceManaged).value / "scalapb"))

// ==========================
// Kafka
// ==========================
lazy val kafka = (project in file("kafka"))
  .dependsOn(messages)
  .dependsOn(datetime)
  .settings(commonSettings *)
  .settings(name := "nj-kafka")
  .settings(libraryDependencies ++= List(
    ("com.github.fd4s" %% "fs2-kafka" % fs2KafkaV).exclude("org.apache.kafka", "kafka-clients"),
    // java
    "io.confluent"     % "kafka-schema-registry-client" % confluentV,
    "io.confluent"     % "kafka-schema-serializer"      % confluentV,
    "org.apache.kafka" % "kafka-streams"                % kafkaV,
    "ch.qos.logback"   % "logback-classic"              % logbackV % Test
  ) ++ jacksonLib ++ testLib)

// ==========================
// Pipes
// ==========================
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
  "com.nrinaudo" %% "kantan.csv"
//  "com.nrinaudo" %% "kantan.csv-java8",
//  "com.nrinaudo" %% "kantan.csv-generic",
//  "com.nrinaudo" %% "kantan.csv-cats"
).map(_ % kantanV).map(_.cross(CrossVersion.for3Use2_13))

lazy val pipes = (project in file("pipes"))
  .dependsOn(datetime)
  .settings(commonSettings *)
  .settings(name := "nj-pipes")
  .settings(
    libraryDependencies ++= List(
      "co.fs2" %% "fs2-io" % fs2V,
      //  "com.nrinaudo" %% "kantan.csv"              % kantanV,
      "com.indoorvivants" %% "scala-uri"          % "4.2.0",
      "com.thesamet.scalapb" %% "scalapb-runtime" % "0.11.20",
      "io.circe" %% "circe-jawn"                  % circeV,
      "org.typelevel" %% "jawn-fs2"               % "2.4.0" % Test,
      "com.sksamuel.avro4s" %% "avro4s-core"      % avro4sV % Test,
      // java
      "software.amazon.awssdk" % "bundle"         % awsV,
      "org.apache.parquet"     % "parquet-common" % parquetV,
      "org.apache.parquet"     % "parquet-hadoop" % parquetV,
      "org.apache.parquet"     % "parquet-avro"   % parquetV,
      "org.apache.avro"        % "avro"           % avroV,
      "org.tukaani"            % "xz"             % "1.12",
      "at.yawk.lz4"            % "lz4-java"       % "1.10.4", // drop-in replacement of org.lz4:lz4-java
      "org.bouncycastle"       % "bcprov-jdk18on" % "1.83" // snyk by hadoop-common
    ) ++ jacksonLib ++ hadoopLib ++ kantanLib ++ testLib
  )
  .settings(dependencyOverrides ++= List(
    "io.airlift"        % "aircompressor"    % "2.0.3", // snyk by parquet-hadoop
    "org.eclipse.jetty" % "jetty-server"     % "12.1.7", // snyk by hadoop-common
    "io.netty"          % "netty-codec-http" % nettyV, // snyk by hadoop-common
    "io.netty"          % "netty-codec-smtp" % nettyV // snyk by hadoop-client
  ))

// ==========================
// Spark
// ==========================
//val sparkLib = List(
//  "org.apache.spark" %% "spark-catalyst",
//  "org.apache.spark" %% "spark-core",
//  "org.apache.spark" %% "spark-sql",
//  "org.apache.spark" %% "spark-avro"
//).map(_ % sparkV).map(_.exclude("org.lz4", "lz4-java"))
//  .map(_.cross(CrossVersion.for3Use2_13))
//
//lazy val spark = (project in file("spark"))
//  .dependsOn(kafka)
//  .dependsOn(pipes)
//  .dependsOn(database)
//  .settings(commonSettings *)
//  .settings(name := "nj-spark")
//  .settings(
//    libraryDependencies ++= List(
//      "com.julianpeeters" %% "avrohugger-core" % "2.16.2" % Test,
//      // "io.circe" %% "circe-shapes"             % circeV   % Test,
//      // java
//      "org.apache.avro" % "avro-mapred"     % avroV,
//      "ch.qos.logback"  % "logback-classic" % logbackV  % Test,
//      "org.postgresql"  % "postgresql"      % postgresV % Test
//    ) ++ jacksonLib ++ sparkLib ++ testLib
//  )
//  .settings(dependencyOverrides ++= List(
//    "org.eclipse.jetty"        % "jetty-server"     % "12.1.7", // snyk by hadoop-common
//    "io.netty"                 % "netty-codec-http" % nettyV, // snyk by spark-sql
//    "org.apache.logging.log4j" % "log4j-core"       % "2.25.3", // snyk by spark-sql
//    "io.airlift"               % "aircompressor"    % "2.0.3" // snyk by spark-sql
//  ))

// ==========================
// Example
// ==========================
lazy val example = (project in file("example"))
  .dependsOn(common)
  .dependsOn(datetime)
  .dependsOn(http)
  .dependsOn(aws)
  .dependsOn(messages)
  .dependsOn(pipes)
  .dependsOn(kafka)
  .dependsOn(database)
  // .dependsOn(spark)
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

// ==========================
// Nanjin
// ==========================
lazy val nanjin =
  (project in file("."))
    .settings(commonSettings *)
    .aggregate(
      common,
      datetime,
      http,
      aws,
      messages,
      pipes,
      kafka,
      database,
      //    spark,
      guard,
      observer_aws,
      observer_database,
      observer_kafka
    )
