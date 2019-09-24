package com.github.chenharryhua.nanjin.sparkdb

import cats.effect.{Async, Blocker, ContextShift, Resource}
import cats.implicits._
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.{Pipe, Stream}
import io.getquill.codegen.jdbc.SimpleJdbcCodegen
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

  def printCaseClass[F[_]: ContextShift: Async]: F[Unit] = transactor.use {
    _.configure { hikari =>
      Async[F]
        .delay(new SimpleJdbcCodegen(() => hikari.getConnection, ""))
        .map(_.writeStrings.toList.mkString("\n"))
        .map(println)
    }
  }

  def runQuery[F[_]: ContextShift: Async, A](action: ConnectionIO[A]): F[A] =
    transactor.use(_.trans.apply(action))

  def runBatch[F[_]: ContextShift: Async, A](
    f: List[A] => ConnectionIO[List[Long]]): Pipe[F, A, List[Long]] =
    (src: Stream[F, A]) =>
      for {
        xa <- Stream.resource(transactor)
        data <- src.chunkN(1000)
        rst <- Stream.eval(xa.trans.apply(f(data.toList)))
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
