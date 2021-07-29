package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Concurrent, Resource}
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.{Chunk, Pipe, Stream}
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
import monocle.macros.Lenses

import scala.concurrent.duration.{DurationInt, FiniteDuration}

/** [[https://tpolecat.github.io/doobie/]]
  */
sealed abstract class DatabaseSettings(username: Username, password: Password) extends Serializable {
  def database: DatabaseName
  def hikariConfig: HikariConfig

  def withPassword(psw: String): DatabaseSettings
  def withUsername(un: String): DatabaseSettings

  final def transactorResource[F[_]: Async]: Resource[F, HikariTransactor[F]] =
    ExecutionContexts.fixedThreadPool[F](8).flatMap { threadPool =>
      HikariTransactor.fromHikariConfig[F](hikariConfig, threadPool)
    }

  final def transactorStream[F[_]: Async]: Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorResource)

  final def genCaseClass[F[_]: Async]: F[String] =
    Resource.unit[F].use { blocker =>
      transactorResource[F].use {
        _.configure { hikari =>
          Async[F]
            .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
            .map(_.writeStrings.toList.mkString("\n"))
        }
      }
    }

  final def runQuery[F[_]: Async, A](action: ConnectionIO[A]): F[A] =
    transactorResource[F].use(_.trans.apply(action))

  final def runStream[F[_]: Async, A](script: Stream[ConnectionIO, A]): Stream[F, A] =
    transactorStream[F].flatMap(_.transP.apply(script))

  final def runBatch[F[_]: Async, A, B](
    f: A => ConnectionIO[B],
    batchSize: Int,
    duration: FiniteDuration): Pipe[F, A, Chunk[B]] =
    (src: Stream[F, A]) =>
      for {
        xa <- transactorStream[F]
        data <- src.groupWithin(batchSize, duration)
        rst <- Stream.eval(xa.trans.apply(data.traverse(f)(doobie.free.connection.WeakAsyncConnectionIO)))
      } yield rst

  final def runBatch[F[_]: Concurrent: Async, A, B](f: A => ConnectionIO[B]): Pipe[F, A, Chunk[B]] =
    runBatch[F, A, B](f, batchSize = 1000, duration = 5.seconds)
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

  override val hikariConfig: HikariConfig = {
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

  override val hikariConfig: HikariConfig = {
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

  override val hikariConfig: HikariConfig = {
    val cfg = new HikariConfig()
    cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    cfg.setJdbcUrl(Protocols.SqlServer.url(host, Some(port)) + s";databaseName=${database.value}")
    cfg.setUsername(username.value)
    cfg.setPassword(password.value)
    cfg
  }
}
