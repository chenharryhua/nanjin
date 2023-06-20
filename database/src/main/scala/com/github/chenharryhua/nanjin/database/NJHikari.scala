package com.github.chenharryhua.nanjin.database

import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig

/** [[https://tpolecat.github.io/doobie/]]
  * @param database
  *   Postgres, Redshit or Sql Server
  * @param cfg
  *   initial HikariConfig
  * @param updateOps
  *   set operations apply to the initial config
  */
sealed abstract class NJHikari[DB](
  val database: DB,
  cfg: HikariConfig,
  updateOps: List[HikariConfig => Unit]) {

  final def set(f: HikariConfig => Unit): NJHikari[DB] =
    new NJHikari[DB](database, cfg, f :: updateOps) {}

  final lazy val hikariConfig: HikariConfig = {
    updateOps.reverse.foreach(_(cfg))
    cfg.validate()
    cfg
  }
}

object NJHikari {
  def apply(db: Postgres): NJHikari[Postgres] = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("org.postgresql.Driver")
      cfg.setJdbcUrl(Protocols.Postgres.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
    new NJHikari[Postgres](db, initConfig, Nil) {}
  }

  def apply(db: Redshift): NJHikari[Redshift] = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.amazon.redshift.jdbc42.Driver")
      cfg.setJdbcUrl(Protocols.Redshift.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg.addDataSourceProperty("ssl", "true")
      cfg.addDataSourceProperty("sslfactory", "com.amazon.redshift.ssl.NonValidatingFactory")
      cfg
    }
    new NJHikari[Redshift](db, initConfig, Nil) {}
  }

  def apply(db: SqlServer): NJHikari[SqlServer] = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      cfg.setJdbcUrl(Protocols.SqlServer.url(db.host, Some(db.port)) + s";databaseName=${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
    new NJHikari[SqlServer](db, initConfig, Nil) {}
  }
}
