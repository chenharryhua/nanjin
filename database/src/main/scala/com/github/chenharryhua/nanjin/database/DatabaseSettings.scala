package com.github.chenharryhua.nanjin.database

import cats.effect.{Async, Blocker, Concurrent, ContextShift, Resource, Timer}
import cats.implicits._
import doobie.free.connection.{AsyncConnectionIO, ConnectionIO}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.{Chunk, Pipe, Stream}
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
import monocle.macros.Lenses

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed abstract class DatabaseSettings(username: Username, password: Password) {
  def driver: DriverString
  def connStr: ConnectionString

  final def show: String =
    s"""
       |database settings:
       |driver:  ${driver.value}
       |connStr: ${connStr.value}
       |""".stripMargin

  final def transactorResource[F[_]: ContextShift: Async](
    blocker: Blocker): Resource[F, HikariTransactor[F]] =
    for {
      threadPool <- ExecutionContexts.fixedThreadPool[F](8)
      xa <- HikariTransactor.newHikariTransactor[F](
        driver.value,
        connStr.value,
        username.value,
        password.value,
        threadPool,
        blocker
      )
    } yield xa

  final def transactorStream[F[_]: ContextShift: Async](
    blocker: Blocker): Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorResource(blocker))

  final def genCaseClass[F[_]: ContextShift: Async]: F[String] =
    Blocker[F].use { blocker =>
      transactorResource[F](blocker).use {
        _.configure { hikari =>
          Async[F]
            .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
            .map(_.writeStrings.toList.mkString("\n"))
        }
      }
    }

  final def runQuery[F[_]: ContextShift: Async, A](
    blocker: Blocker,
    action: ConnectionIO[A]): F[A] =
    transactorResource[F](blocker).use(_.trans.apply(action))

  final def runStream[F[_]: ContextShift: Async, A](
    blocker: Blocker,
    script: Stream[ConnectionIO, A]): Stream[F, A] =
    transactorStream[F](blocker).flatMap(_.transP.apply(script))

  final def runBatch[F[_]: ContextShift: Concurrent: Timer, A, B](
    blocker: Blocker,
    f: A => ConnectionIO[B],
    batchSize: Int,
    duration: FiniteDuration
  ): Pipe[F, A, Chunk[B]] =
    (src: Stream[F, A]) =>
      for {
        xa <- transactorStream[F](blocker)
        data <- src.groupWithin(batchSize, duration)
        rst <- Stream.eval(xa.trans.apply(data.traverse(f)(AsyncConnectionIO)))
      } yield rst

  final def runBatch[F[_]: ContextShift: Concurrent: Timer, A, B](
    blocker: Blocker,
    f: A => ConnectionIO[B]): Pipe[F, A, Chunk[B]] =
    runBatch[F, A, B](blocker, f, batchSize = 1000, duration = 5.seconds)
}

@Lenses final case class Postgres(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {
  private val url: String                = Protocols.Postgres.url(host, Some(port)) + s"/${database.value}"
  private val credential: String         = s"user=${username.value}&password=${password.value}"
  override val connStr: ConnectionString = ConnectionString(s"$url?$credential")
  override val driver: DriverString      = DriverString("org.postgresql.Driver")

}

@Lenses final case class Redshift(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {
  private val url: String        = Protocols.Redshift.url(host, Some(port)) + s"/${database.value}"
  private val credential: String = s"user=${username.value}&password=${password.value}"
  private val ssl: String        = "ssl=true&sslfactory=com.amazon.redshift.ssl.NonValidatingFactory"

  override val connStr: ConnectionString = ConnectionString(s"$url?$credential&$ssl")
  override val driver: DriverString      = DriverString("com.amazon.redshift.jdbc42.Driver")

}

@Lenses final case class SqlServer(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {

  private val url: String = Protocols.SqlServer.url(host, Some(port))

  override val connStr: ConnectionString =
    ConnectionString(url + s";databaseName=${database.value}")

  override val driver: DriverString =
    DriverString("com.microsoft.sqlserver.jdbc.SQLServerDriver")

}
