package com.github.chenharryhua.nanjin.database

import cats.effect.{Concurrent, Resource}
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.database.Postgres
import fs2.io.net.Network
import natchez.Trace
import skunk.{SSL, Session}
import skunk.util.Typer

final case class SkunkSession private (
  postgres: Postgres,
  max: Int,
  debug: Boolean,
  strategy: Typer.Strategy,
  ssl: SSL,
  parameters: Map[String, String],
  commandCache: Int,
  queryCache: Int) {
  def withMaxSessions(size: Int): SkunkSession       = copy(max = size)
  def withStrategy(ts: Typer.Strategy): SkunkSession = copy(strategy = ts)
  def withSSL(ssl: SSL): SkunkSession                = copy(ssl = ssl)
  def withCommandCache(size: Int): SkunkSession      = copy(commandCache = size)
  def withQueryCache(size: Int): SkunkSession        = copy(queryCache = size)
  def withDebug: SkunkSession                        = copy(debug = true)

  def withParameters(ps: Map[String, String]): SkunkSession = copy(parameters = ps)
  def addParameter(k: String, v: String): SkunkSession      = copy(parameters = parameters + (k -> v))

  def pooled[F[_]: Concurrent: Trace: Network: Console]: Resource[F, Resource[F, Session[F]]] =
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

  def single[F[_]: Concurrent: Trace: Network: Console]: Resource[F, Session[F]] =
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

object SkunkSession {
  def apply(postgres: Postgres): SkunkSession = SkunkSession(
    postgres = postgres,
    max = 1,
    debug = false,
    strategy = Typer.Strategy.BuiltinsOnly,
    ssl = SSL.None,
    parameters = Session.DefaultConnectionParameters,
    commandCache = 1024,
    queryCache = 1024
  )
}
