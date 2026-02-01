package com.github.chenharryhua.nanjin.database

import cats.effect.Resource
import cats.effect.kernel.Temporal
import cats.effect.std.Console
import com.github.chenharryhua.nanjin.common.database.Postgres
import fs2.io.net.{Network, SocketOption}
import natchez.Trace
import skunk.{SSL, Session}
import skunk.util.Typer

import scala.concurrent.duration.Duration

// https://github.com/tpolecat/skunk
final case class SkunkSession[F[_]](
  postgres: Postgres,
  max: Int,
  debug: Boolean,
  strategy: Typer.Strategy,
  ssl: SSL,
  parameters: Map[String, String],
  socketOptions: List[SocketOption],
  commandCache: Int,
  queryCache: Int,
  parseCache: Int,
  readTimeout: Duration,
  trace: Option[Trace[F]]) {

  def withMaxSessions(size: Int): SkunkSession[F] = copy(max = size)
  def withStrategy(ts: Typer.Strategy): SkunkSession[F] = copy(strategy = ts)
  def withSSL(ssl: SSL): SkunkSession[F] = copy(ssl = ssl)

  def withCommandCache(size: Int): SkunkSession[F] = copy(commandCache = size)
  def withQueryCache(size: Int): SkunkSession[F] = copy(queryCache = size)
  def withParseCache(size: Int): SkunkSession[F] = copy(parseCache = size)

  def withDebug: SkunkSession[F] = copy(debug = true)

  def withReadTimeout(duration: Duration): SkunkSession[F] = copy(readTimeout = duration)
  def withSocketOptions(so: List[SocketOption]): SkunkSession[F] = copy(socketOptions = so)

  def withParameters(ps: Map[String, String]): SkunkSession[F] = copy(parameters = ps)
  def addParameter(k: String, v: String): SkunkSession[F] = copy(parameters = parameters + (k -> v))

  def withTrace(trace: Trace[F]): SkunkSession[F] = copy(trace = Some(trace))

  def pooled(implicit C: Temporal[F], N: Network[F], S: Console[F]): Resource[F, Session[F]] = {
    implicit val tc: Trace[F] = trace.getOrElse(natchez.Trace.Implicits.noop)
    Session
      .pooled(
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
        socketOptions = socketOptions,
        commandCache = commandCache,
        queryCache = queryCache,
        parseCache = parseCache,
        readTimeout = readTimeout
      )
      .flatMap(identity)
  }

  def single(implicit C: Temporal[F], N: Network[F], S: Console[F]): Resource[F, Session[F]] = {
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
      queryCache = queryCache,
      parseCache = parseCache,
      readTimeout = readTimeout
    )
  }
}

object SkunkSession {
  def apply[F[_]](postgres: Postgres): SkunkSession[F] =
    SkunkSession[F](
      postgres = postgres,
      max = 1,
      debug = false,
      strategy = Typer.Strategy.BuiltinsOnly,
      ssl = SSL.None,
      parameters = Session.DefaultConnectionParameters,
      socketOptions = Session.DefaultSocketOptions,
      commandCache = 1024,
      queryCache = 1024,
      parseCache = 1024,
      readTimeout = Duration.Inf,
      trace = None
    )
}
