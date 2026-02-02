package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Resource}
import cats.implicits.{catsSyntaxApplicativeError, toFunctorOps}
import com.github.chenharryhua.nanjin.common.database.*
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.implicits.toSqlInterpolator
import doobie.util.log.LogHandler
import fs2.Stream

/** `https://tpolecat.github.io/doobie/`
  * @param cfg
  *   initial HikariConfig
  * @param updateOps
  *   set operations apply to the initial config
  */
sealed abstract class DBConfig(cfg: HikariConfig, updateOps: List[HikariConfig => Unit]) {

  final def set(f: HikariConfig => Unit): DBConfig =
    new DBConfig(cfg, f :: updateOps) {}

  final lazy val hikariConfig: HikariConfig = {
    val cfgCopy = new HikariConfig()
    cfg.copyStateTo(cfgCopy)
    updateOps.reverse.foreach(_(cfgCopy))
    cfgCopy.validate()
    cfgCopy
  }

  final def transactorR[F[_]: Async](logHandler: Option[LogHandler[F]]): Resource[F, HikariTransactor[F]] =
    HikariTransactor.fromHikariConfig[F](hikariConfig, logHandler)

  final def transactorS[F[_]: Async](logHandler: Option[LogHandler[F]]): Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorR(logHandler))

  final def testConnection[F[_]: Async]: F[Boolean] =
    transactorR[F](None).use(_.trans.apply(sql"select 42".query[Int].unique)).attempt.map(_.isRight)
}

object DBConfig {
  def apply(db: Postgres): DBConfig = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("org.postgresql.Driver")
      cfg.setJdbcUrl(Protocols.Postgres.url(db.host, Some(db.port)) + s"/${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
    new DBConfig(initConfig, Nil) {}
  }

  def apply(db: Redshift): DBConfig = {
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
    new DBConfig(initConfig, Nil) {}
  }

  def apply(db: SqlServer): DBConfig = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.microsoft.sqlserver.jdbc.SQLServerDriver")
      cfg.setJdbcUrl(Protocols.SqlServer.url(db.host, Some(db.port)) + s";databaseName=${db.database.value}")
      cfg.setUsername(db.username.value)
      cfg.setPassword(db.password.value)
      cfg
    }
    new DBConfig(initConfig, Nil) {}
  }
}
