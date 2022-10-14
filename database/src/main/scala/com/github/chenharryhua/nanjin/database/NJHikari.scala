package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.Stream

/** [[https://tpolecat.github.io/doobie/]]
  */
sealed abstract class NJHikari[DB](val database: DB) {
  def hikariConfig: HikariConfig

  final def set(f: HikariConfig => Unit): this.type = {
    f(hikariConfig)
    this
  }

  /** use one of doobie.util.ExecutionContexts
    */

  final def transactorResource[F[_]: Async]: Resource[F, HikariTransactor[F]] =
    ExecutionContexts
      .fixedThreadPool[F](hikariConfig.getMaximumPoolSize)
      .flatMap(tp => HikariTransactor.fromHikariConfig[F](hikariConfig, tp))

  final def transactorStream[F[_]: Async]: Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorResource)
}

object NJHikari {
  def apply(db: Postgres): NJHikari[Postgres] = new NJHikari[Postgres](db) {
    override val hikariConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("org.postgresql.Driver")
      cfg.setJdbcUrl(Protocols.Postgres.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
  }

  def apply(db: Redshift): NJHikari[Redshift] = new NJHikari[Redshift](db) {
    override val hikariConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.amazon.redshift.jdbc42.Driver")
      cfg.setJdbcUrl(Protocols.Redshift.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg.addDataSourceProperty("ssl", "true")
      cfg.addDataSourceProperty("sslfactory", "com.amazon.redshift.ssl.NonValidatingFactory")
      cfg
    }
  }

  def apply(db: SqlServer): NJHikari[SqlServer] = new NJHikari[SqlServer](db) {
    override val hikariConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      cfg.setJdbcUrl(Protocols.SqlServer.url(db.host, Some(db.port)) + s";databaseName=${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
  }
}
