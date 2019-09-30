package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Async, Blocker, Concurrent, ContextShift, Resource, Timer}
import cats.implicits._
import doobie.free.connection.{AsyncConnectionIO, ConnectionIO}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.{Chunk, Pipe, Stream}
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
import monocle.macros.Lenses

import scala.concurrent.duration.DurationInt

final case class Username(value: String) extends AnyVal
final case class Password(value: String) extends AnyVal
final case class DatabaseName(value: String) extends AnyVal
final case class DatabaseHost(value: String) extends AnyVal
final case class DatabasePort(value: Int) extends AnyVal

final case class DatabaseConnectionString(value: String) extends AnyVal
final case class DatabaseDriverString(value: String) extends AnyVal

sealed abstract class DatabaseSettings(username: Username, password: Password) {
  def driver: DatabaseDriverString
  def connStr: DatabaseConnectionString

  final def show: String =
    s"""
       |database settings:
       |driver:  ${driver.value}
       |connStr: ${connStr.value}
       |""".stripMargin

  final def transactorResource[F[_]: ContextShift: Async]: Resource[F, HikariTransactor[F]] =
    for {
      theadPool <- ExecutionContexts.fixedThreadPool[F](8)
      cachedPool <- ExecutionContexts.cachedThreadPool[F]
      xa <- HikariTransactor.newHikariTransactor[F](
        driver.value,
        connStr.value,
        username.value,
        password.value,
        theadPool,
        Blocker.liftExecutionContext(cachedPool)
      )
    } yield xa

  final def transactorStream[F[_]: ContextShift: Async]: Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorResource)

  final def printCaseClass[F[_]: ContextShift: Async]: F[Unit] = transactorResource.use {
    _.configure { hikari =>
      Async[F]
        .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
        .map(_.writeStrings.toList.mkString("\n"))
        .map(println)
    }
  }

  final def runQuery[F[_]: ContextShift: Async, A](action: ConnectionIO[A]): F[A] =
    transactorResource.use(_.trans.apply(action))

  final def runBatch[F[_]: ContextShift: Concurrent: Timer, A, B](
    f: A => ConnectionIO[B],
    batchSize: Int = 1000): Pipe[F, A, Chunk[B]] =
    (src: Stream[F, A]) =>
      for {
        xa <- transactorStream
        data <- src.groupWithin(batchSize, 5.seconds)
        rst <- Stream.eval(xa.trans.apply(data.traverse(f)(AsyncConnectionIO)))
      } yield rst
}

@Lenses final case class Postgres(
  username: Username,
  password: Password,
  host: DatabaseHost,
  port: DatabasePort,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {
  private val url: String                        = s"jdbc:postgresql://${host.value}:${port.value}/${database.value}"
  private val credential: String                 = s"user=${username.value}&password=${password.value}"
  override val connStr: DatabaseConnectionString = DatabaseConnectionString(s"$url?$credential")
  override val driver: DatabaseDriverString      = DatabaseDriverString("org.postgresql.Driver")
}

@Lenses final case class Redshift(
  username: Username,
  password: Password,
  host: DatabaseHost,
  port: DatabasePort,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {
  private val url: String        = s"jdbc:redshift://${host.value}:${port.value}/${database.value}"
  private val credential: String = s"user=${username.value}&password=${password.value}"
  private val ssl: String        = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory"
  override val connStr: DatabaseConnectionString =
    DatabaseConnectionString(s"$url?$credential&$ssl")
  override val driver: DatabaseDriverString =
    DatabaseDriverString("com.amazon.redshift.jdbc42.Driver")
}

@Lenses final case class SqlServer(
  username: Username,
  password: Password,
  host: DatabaseHost,
  port: DatabasePort,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {
  override val connStr: DatabaseConnectionString =
    DatabaseConnectionString(
      s"jdbc:sqlserver://${host.value}:${port.value};databaseName=${database.value}")
  override val driver: DatabaseDriverString =
    DatabaseDriverString("com.microsoft.sqlserver.jdbc.SQLServerDriver")
}
