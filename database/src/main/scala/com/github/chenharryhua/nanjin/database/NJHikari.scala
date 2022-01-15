package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Resource}
import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig
import doobie.free.connection.ConnectionIO
import doobie.hikari.HikariTransactor
import doobie.util.ExecutionContexts
import fs2.Stream

/** [[https://tpolecat.github.io/doobie/]]
  */
sealed trait NJHikari[DB] {
  def hikariConfig(db: DB): HikariConfig

  final def transactorResource[F[_]: Async](db: DB): Resource[F, HikariTransactor[F]] =
    ExecutionContexts.fixedThreadPool[F](8).flatMap { threadPool =>
      HikariTransactor.fromHikariConfig[F](hikariConfig(db), threadPool)
    }

  final def transactorStream[F[_]: Async](db: DB): Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorResource(db))

  final def runQuery[F[_]: Async, A](db: DB)(action: ConnectionIO[A]): F[A] =
    transactorResource[F](db).use(_.trans.apply(action))
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
