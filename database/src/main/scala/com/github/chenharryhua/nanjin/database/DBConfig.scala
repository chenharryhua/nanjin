package com.github.chenharryhua.nanjin.database

import cats.effect.kernel.{Async, Resource}
import cats.syntax.applicativeError.given
import cats.syntax.functor.given
import com.zaxxer.hikari.HikariConfig
import org.typelevel.doobie.hikari.HikariTransactor
import org.typelevel.doobie.syntax.string.toSqlInterpolator
import org.typelevel.doobie.util.log.LogHandler
import fs2.Stream

/** An immutable, composable builder for a doobie `HikariTransactor`, wrapping a HikariCP `HikariConfig`.
  *
  * Construct one from a database descriptor via the DBConfig.apply overloads (Postgres, Redshift, SqlServer),
  * which seed driver, JDBC URL, and credentials. Further tuning is applied with `set`, which records a
  * mutation rather than mutating in place: each `set` returns a new `DBConfig` carrying an additional
  * operation. The operations are replayed, in insertion order, onto a fresh copy of the initial config the
  * first time `hikariConfig` is forced.
  *
  * See `https://tpolecat.github.io/doobie/`.
  *
  * @param cfg
  *   the initial HikariCP configuration
  * @param updateOps
  *   pending mutations to apply to a copy of `cfg`, stored newest-first and replayed in insertion order
  */
sealed abstract class DBConfig(cfg: HikariConfig, updateOps: List[HikariConfig => Unit]) {

  /** Record a mutation of the underlying `HikariConfig` (e.g. `_.setMaximumPoolSize(10)`), returning a new
    * `DBConfig`. The function is not run now; it is applied when `hikariConfig` is built.
    */
  final def set(f: HikariConfig => Unit): DBConfig =
    new DBConfig(cfg, f :: updateOps) {}

  /** The effective `HikariConfig`: a fresh copy of the initial config with every recorded `set` applied in
    * insertion order, then validated. Computed once and cached.
    */
  final lazy val hikariConfig: HikariConfig = {
    val cfgCopy = new HikariConfig()
    cfg.copyStateTo(cfgCopy)
    updateOps.reverse.foreach(_(cfgCopy))
    cfgCopy.validate()
    cfgCopy
  }

  /** A `HikariTransactor` as a `Resource`; the pool is created on acquire and shut down on release.
    *
    * @param logHandler
    *   optional doobie `LogHandler` for statement logging
    */
  final def transactorR[F[_]: Async](logHandler: Option[LogHandler[F]]): Resource[F, HikariTransactor[F]] =
    HikariTransactor.fromHikariConfig[F](hikariConfig, logHandler)

  /** `transactorR` as a single-element `Stream`. */
  final def transactorS[F[_]: Async](logHandler: Option[LogHandler[F]]): Stream[F, HikariTransactor[F]] =
    Stream.resource(transactorR(logHandler))

  /** Open a transactor and run `select 42`, returning `true` on success and `false` on any error. Useful as a
    * startup connectivity probe.
    */
  final def testConnection[F[_]: Async]: F[Boolean] =
    transactorR[F](None).use(_.trans.apply(sql"select 42".query[Int].unique)).attempt.map(_.isRight)
}

object DBConfig {
  def apply(db: Postgres): DBConfig = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("org.postgresql.Driver")
      cfg.setJdbcUrl(Protocols.Postgres.url(db.host, Some(db.port)) + s"/${db.database}")
      cfg.setUsername(db.username)
      cfg.setPassword(db.password.value)
      cfg
    }
    new DBConfig(initConfig, Nil) {}
  }

  def apply(db: Redshift): DBConfig = {
    val initConfig: HikariConfig = {
      val cfg = new HikariConfig
      cfg.setDriverClassName("com.amazon.redshift.jdbc42.Driver")
      cfg.setJdbcUrl(Protocols.Redshift.url(db.host, Some(db.port)) + s"/${db.database}")
      cfg.setUsername(db.username)
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
      cfg.setJdbcUrl(Protocols.SqlServer.url(db.host, Some(db.port)) + s";databaseName=${db.database}")
      cfg.setUsername(db.username)
      cfg.setPassword(db.password.value)
      cfg
    }
    new DBConfig(initConfig, Nil) {}
  }
}
