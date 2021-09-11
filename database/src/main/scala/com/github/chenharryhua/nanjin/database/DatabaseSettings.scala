package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.Stream
import monocle.macros.Lenses

/** [[https://tpolecat.github.io/doobie/]]
  */
sealed trait DatabaseSettings extends Serializable {
  def username: Username
  def password: Password
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

  final def runQuery[F[_]: Async, A](action: ConnectionIO[A]): F[A] =
    transactorResource[F].use(_.trans.apply(action))
}

@Lenses final case class Postgres(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings {

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
    extends DatabaseSettings {

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
    extends DatabaseSettings {

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
