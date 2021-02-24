package com.github.chenharryhua.nanjin.database

import com.zaxxer.hikari.HikariConfig
import monocle.macros.Lenses

sealed trait DatabaseSettings extends Serializable {
  def username: Username
  def password: Password
  def host: Host
  def port: Port
  def database: DatabaseName
  
  def hikariConfig: HikariConfig

  def withUsername(un: String): DatabaseSettings
  def withPassword(psw: String): DatabaseSettings
  def withHost(h: String): DatabaseSettings
  def withPort(p: Int): DatabaseSettings
  def withDatabase(db: String): DatabaseSettings
}

@Lenses final case class Postgres(
  username: Username,
  password: Password,
  host: Host,
  port: Port,
  database: DatabaseName)
    extends DatabaseSettings {

  override def withPassword(psw: String): Postgres = Postgres.password.set(Password.unsafeFrom(psw))(this)
  override def withUsername(un: String): Postgres  = Postgres.username.set(Username.unsafeFrom(un))(this)
  override def withHost(h: String): Postgres       = Postgres.host.set(Host.unsafeFrom(h))(this)
  override def withPort(p: Int): Postgres          = Postgres.port.set(Port.unsafeFrom(p))(this)
  override def withDatabase(db: String): Postgres  = Postgres.database.set(DatabaseName.unsafeFrom(db))(this)

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

  override def withPassword(psw: String): Redshift = Redshift.password.set(Password.unsafeFrom(psw))(this)
  override def withUsername(un: String): Redshift  = Redshift.username.set(Username.unsafeFrom(un))(this)
  override def withHost(h: String): Redshift       = Redshift.host.set(Host.unsafeFrom(h))(this)
  override def withPort(p: Int): Redshift          = Redshift.port.set(Port.unsafeFrom(p))(this)
  override def withDatabase(db: String): Redshift  = Redshift.database.set(DatabaseName.unsafeFrom(db))(this)

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

  override def withPassword(psw: String): SqlServer = SqlServer.password.set(Password.unsafeFrom(psw))(this)
  override def withUsername(un: String): SqlServer  = SqlServer.username.set(Username.unsafeFrom(un))(this)
  override def withHost(h: String): SqlServer       = SqlServer.host.set(Host.unsafeFrom(h))(this)
  override def withPort(p: Int): SqlServer          = SqlServer.port.set(Port.unsafeFrom(p))(this)
  override def withDatabase(db: String): SqlServer  = SqlServer.database.set(DatabaseName.unsafeFrom(db))(this)

  override val hikariConfig: HikariConfig = {
    val cfg = new HikariConfig()
    cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
    cfg.setJdbcUrl(Protocols.SqlServer.url(host, Some(port)) + s";databaseName=${database.value}")
    cfg.setUsername(username.value)
    cfg.setPassword(password.value)
    cfg
  }
}
