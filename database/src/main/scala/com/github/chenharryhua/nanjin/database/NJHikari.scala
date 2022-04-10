package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import fs2.Stream

import scala.concurrent.ExecutionContext

/** [[https://tpolecat.github.io/doobie/]]
  */
sealed trait NJHikari[DB] {
  def hikariConfig(db: DB): HikariConfig

  final def transactorResource[F[_]: Async](
    db: DB,
    threadPool: Resource[F, ExecutionContext]): Resource[F, HikariTransactor[F]] =
    threadPool.flatMap(tp => HikariTransactor.fromHikariConfig[F](hikariConfig(db), tp))

  final def transactorStream[F[_]: Async](
    db: DB,
    threadPool: Resource[F, ExecutionContext]): Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorResource(db, threadPool))
}

object NJHikari {
  def apply[DB](db: DB)(implicit ev: NJHikari[DB]): HikariConfig = ev.hikariConfig(db)
  def apply[DB](implicit ev: NJHikari[DB]): NJHikari[DB]         = ev

  implicit val databaseSettingsPostgres: NJHikari[Postgres] = new NJHikari[Postgres] {
    override def hikariConfig(db: Postgres): HikariConfig = {
      val cfg = new HikariConfig()
      cfg.setDriverClassName("org.postgresql.Driver")
      cfg.setJdbcUrl(Protocols.Postgres.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
  }

  implicit val databaseSettingsRedshift: NJHikari[Redshift] = new NJHikari[Redshift] {
    override def hikariConfig(db: Redshift): HikariConfig = {
      val cfg = new HikariConfig()
      cfg.setDriverClassName("com.amazon.redshift.jdbc42.Driver")
      cfg.setJdbcUrl(Protocols.Redshift.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg.addDataSourceProperty("ssl", "true")
      cfg.addDataSourceProperty("sslfactory", "com.amazon.redshift.ssl.NonValidatingFactory")
      cfg
    }
  }

  implicit val databaseSettingsSqlServer: NJHikari[SqlServer] = new NJHikari[SqlServer] {
    override def hikariConfig(db: SqlServer): HikariConfig = {
      val cfg = new HikariConfig()
      cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      cfg.setJdbcUrl(Protocols.SqlServer.url(db.host, Some(db.port)) + s";databaseName=${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
  }
}
