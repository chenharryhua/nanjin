package com.github.chenharryhua.nanjin.database

import cats.effect.{Concurrent, Resource}
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.database.Postgres
import fs2.io.net.Network
import natchez.Trace
import skunk.{SSL, Session}
import skunk.util.Typer

// https://github.com/tpolecat/skunk
final case class SkunkSession[F[_]] private (
  postgres: Postgres,
  max: Int,
  debug: Boolean,
  strategy: Typer.Strategy,
  ssl: SSL,
  parameters: Map[String, String],
  commandCache: Int,
  queryCache: Int,
  trace: Option[Trace[F]]) {
  def withMaxSessions(size: Int): SkunkSession[F]       = copy(max = size)
  def withStrategy(ts: Typer.Strategy): SkunkSession[F] = copy(strategy = ts)
  def withSSL(ssl: SSL): SkunkSession[F]                = copy(ssl = ssl)
  def withCommandCache(size: Int): SkunkSession[F]      = copy(commandCache = size)
  def withQueryCache(size: Int): SkunkSession[F]        = copy(queryCache = size)
  def withDebug: SkunkSession[F]                        = copy(debug = true)

  def withParameters(ps: Map[String, String]): SkunkSession[F] = copy(parameters = ps)
  def addParameter(k: String, v: String): SkunkSession[F]      = copy(parameters = parameters + (k -> v))

  def withTrace(trace: Trace[F]): SkunkSession[F] = copy(trace = Some(trace))

  def pooled(implicit C: Concurrent[F], N: Network[F], S: Console[F]): Resource[F, Resource[F, Session[F]]] = {
    implicit val tc: Trace[F] = trace.getOrElse(natchez.Trace.Implicits.noop)
    Session.pooled(
      host = postgres.host.value,
      port = postgres.port.value,
      user = postgres.username.value,
      database = postgres.database.value,
      password = Some(postgres.password.value),
      max = max,
      debug = debug,
      strategy = strategy,
      ssl = ssl,
      parameters = parameters,
      commandCache = commandCache,
      queryCache = queryCache
    )
  }

  def single(implicit C: Concurrent[F], N: Network[F], S: Console[F]): Resource[F, Session[F]] = {
    implicit val tc: Trace[F] = trace.getOrElse(natchez.Trace.Implicits.noop)
    Session.single(
      host = postgres.host.value,
      port = postgres.port.value,
      user = postgres.username.value,
      database = postgres.database.value,
      password = Some(postgres.password.value),
      debug = debug,
      strategy = strategy,
      ssl = ssl,
      parameters = parameters,
      commandCache = commandCache,
      queryCache = queryCache
    )
  }
}

object SkunkSession {
  def apply[F[_]](postgres: Postgres): SkunkSession[F] = SkunkSession[F](
    postgres = postgres,
    max = 1,
    debug = false,
    strategy = Typer.Strategy.BuiltinsOnly,
    ssl = SSL.None,
    parameters = Session.DefaultConnectionParameters,
    commandCache = 1024,
    queryCache = 1024,
    None
  )
}
