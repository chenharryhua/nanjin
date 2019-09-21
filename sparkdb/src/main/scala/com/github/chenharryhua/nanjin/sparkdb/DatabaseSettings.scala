package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Async, Blocker, ConcurrentEffect, ContextShift, Resource}
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import monocle.macros.Lenses

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

  def transactor[F[_]: ContextShift: Async]: Resource[F, HikariTransactor[F]] =
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
  override val connStr: DatabaseConnectionString = DatabaseConnectionString(
    s"$url?$credential&$ssl")
  override val driver: DatabaseDriverString = DatabaseDriverString(
    "com.amazon.redshift.jdbc42.Driver")
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
