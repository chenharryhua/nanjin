package com.github.chenharryhua.nanjin.database

import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig

/** [[https://tpolecat.github.io/doobie/]]
  * @param cfg
  *   initial HikariConfig
  * @param updateOps
  *   set operations apply to the initial config
  */
sealed abstract class NJHikari(cfg: HikariConfig, updateOps: List[HikariConfig => Unit]) {

  final def set(f: HikariConfig => Unit): NJHikari =
    new NJHikari(cfg, f :: updateOps) {}

  final lazy val hikariConfig: HikariConfig = {
    updateOps.reverse.foreach(_(cfg))
    cfg.validate()
    cfg
  }
}

object NJHikari {
  def apply(db: Postgres): NJHikari = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("org.postgresql.Driver")
      cfg.setJdbcUrl(Protocols.Postgres.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
    new NJHikari(initConfig, Nil) {}
  }

  def apply(db: Redshift): NJHikari = {
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
    new NJHikari(initConfig, Nil) {}
  }

  def apply(db: SqlServer): NJHikari = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      cfg.setJdbcUrl(Protocols.SqlServer.url(db.host, Some(db.port)) + s";databaseName=${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
    new NJHikari(initConfig, Nil) {}
  }
}
