package com.github.chenharryhua.nanjin.database

import cats.effect.{Async, Blocker, Concurrent, ContextShift, Resource, Timer}
import cats.syntax.all._
import com.zaxxer.hikari.HikariConfig
import doobie.free.connection.{AsyncConnectionIO, ConnectionIO}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.{Chunk, Pipe, Stream}
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
import monocle.macros.Lenses

import scala.concurrent.duration.{DurationInt, FiniteDuration}

sealed abstract class DatabaseSettings(username: Username, password: Password)
    extends Serializable {
  def database: DatabaseName
  def config: HikariConfig

  def withPassword(psw: String): DatabaseSettings
  def withUsername(un: String): DatabaseSettings

  final def transactorResource[F[_]: ContextShift: Async](
    blocker: Blocker): Resource[F, HikariTransactor[F]] =
    ExecutionContexts.fixedThreadPool[F](8).flatMap { threadPool =>
      HikariTransactor.fromHikariConfig[F](config, threadPool, blocker)
    }

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

  override def withPassword(psw: String): Postgres =
    Postgres.password.set(Password.unsafeFrom(psw))(this)

  override def withUsername(un: String): Postgres =
    Postgres.username.set(Username.unsafeFrom(un))(this)

  override val config: HikariConfig = {
    val cfg = new HikariConfig()
    cfg.setDriverClassName("org.postgresql.Driver")
    cfg.setJdbcUrl(Protocols.Postgres.url(host, Some(port)) + s"/${database.value}")
    cfg.setUsername(username.value)
    cfg.setPassword(password.value)
    cfg
  }
}

@Lenses final case class Redshift(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {

  override def withPassword(psw: String): Redshift =
    Redshift.password.set(Password.unsafeFrom(psw))(this)

  override def withUsername(un: String): Redshift =
    Redshift.username.set(Username.unsafeFrom(un))(this)

  override val config: HikariConfig = {
    val cfg = new HikariConfig()
    cfg.setDriverClassName("com.amazon.redshift.jdbc42.Driver")
    cfg.setJdbcUrl(Protocols.Redshift.url(host, Some(port)) + s"/${database.value}")
    cfg.setUsername(username.value)
    cfg.setPassword(password.value)
    cfg.addDataSourceProperty("ssl", "true")
    cfg.addDataSourceProperty("sslfactory", "com.amazon.redshift.ssl.NonValidatingFactory")
    cfg
  }
}

@Lenses final case class SqlServer(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings(username, password) {

  override def withPassword(psw: String): SqlServer =
    SqlServer.password.set(Password.unsafeFrom(psw))(this)

  override def withUsername(un: String): SqlServer =
    SqlServer.username.set(Username.unsafeFrom(un))(this)

  override val config: HikariConfig = {
    val cfg = new HikariConfig()
    cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    cfg.setJdbcUrl(Protocols.SqlServer.url(host, Some(port)) + s";databaseName=${database.value}")
    cfg.setUsername(username.value)
    cfg.setPassword(password.value)
    cfg
  }
}
