package com.github.chenharryhua.nanjin.sparkafka

import java.util.Properties

import cats.Eval
import com.github.chenharryhua.nanjin.codec.utils
import monocle.macros.Lenses

final case class Username(value: String) extends AnyVal
final case class Password(value: String) extends AnyVal
final case class DatabaseName(value: String) extends AnyVal
final case class DatabaseHost(value: String) extends AnyVal
final case class DatabasePort(value: Int) extends AnyVal
final case class IamRole(value: String) extends AnyVal

final case class DatabaseConnectionString(value: String) extends AnyVal
final case class DatabaseDriverString(value: String) extends AnyVal

sealed abstract class DatabaseSettings(username: Username, password: Password) {
  def driver: DatabaseDriverString
  def connStr: DatabaseConnectionString

  final val connectionProperties: Eval[Properties] =
    Eval.later(
      utils.toProperties(
        Map(
          "user" -> s"${username.value}",
          "password" -> s"${password.value}",
          "Driver" -> driver.value)))

  final def show: String =
    s"""
       |database settings:
       |driver:  ${driver.value}
       |connStr: ${connStr.value}
       |""".stripMargin
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
  database: DatabaseName,
  iamRole: IamRole)
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
