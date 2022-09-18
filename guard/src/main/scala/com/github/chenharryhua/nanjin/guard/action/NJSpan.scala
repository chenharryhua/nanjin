package com.github.chenharryhua.nanjin.guard.action
import cats.Endo
import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref}
import cats.effect.Resource
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.config.{ActionParams, AgentConfig}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.service.ServiceStatus
import fs2.concurrent.Channel
import fs2.Stream
import io.circe.Json
import natchez.{Kernel, Span, TraceValue}

import java.net.URI
import scala.concurrent.Future
import scala.util.control.NonFatal

final class NJSpan[F[_]] private[guard] (
  underlieSpan: Span[F],
  spanName: String,
  metricRegistry: MetricRegistry,
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent],
  agentConfig: AgentConfig)(implicit F: Async[F])
    extends UpdateConfig[AgentConfig, NJSpan[F]] with Span[F] {

  override def updateConfig(f: Endo[AgentConfig]): NJSpan[F] =
    new NJSpan[F](underlieSpan, spanName, metricRegistry, serviceStatus, channel, f(agentConfig))

  def trivial: NJSpan[F]  = updateConfig(_.withLowImportance)
  def silent: NJSpan[F]   = updateConfig(_.withMediumImportance)
  def notice: NJSpan[F]   = updateConfig(_.withHighImportance)
  def critical: NJSpan[F] = updateConfig(_.withCriticalImportance)

  def expensive: NJSpan[F] = updateConfig(_.withExpensive(isCostly = true))
  def cheap: NJSpan[F]     = updateConfig(_.withExpensive(isCostly = false))

  // retries
  def retry[Z](fb: F[Z]): NJRetry0[F, Z] = // 0 arity
    new NJRetry0[F, Z](
      underlieSpan = underlieSpan,
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentConfig.evalConfig, spanName),
      arrow = fb,
      transInput = F.pure(Json.Null),
      transOutput = _ => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, Z](f: A => F[Z]): NJRetry[F, A, Z] =
    new NJRetry[F, A, Z](
      underlieSpan = underlieSpan,
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentConfig.evalConfig, spanName),
      arrow = f,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: A, _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, Z](f: (A, B) => F[Z]): NJRetry[F, (A, B), Z] =
    new NJRetry[F, (A, B), Z](
      underlieSpan = underlieSpan,
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentConfig.evalConfig, spanName),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, Z](f: (A, B, C) => F[Z]): NJRetry[F, (A, B, C), Z] =
    new NJRetry[F, (A, B, C), Z](
      underlieSpan = underlieSpan,
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentConfig.evalConfig, spanName),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): NJRetry[F, (A, B, C, D), Z] =
    new NJRetry[F, (A, B, C, D), Z](
      underlieSpan = underlieSpan,
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentConfig.evalConfig, spanName),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): NJRetry[F, (A, B, C, D, E), Z] =
    new NJRetry[F, (A, B, C, D, E), Z](
      underlieSpan = underlieSpan,
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentConfig.evalConfig, spanName),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D, E), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  // future

  def retryFuture[Z](future: F[Future[Z]]): NJRetry0[F, Z] = // 0 arity
    retry(F.fromFuture(future))

  def retryFuture[A, Z](f: A => Future[Z]): NJRetry[F, A, Z] =
    retry((a: A) => F.fromFuture(F.delay(f(a))))

  def retryFuture[A, B, Z](f: (A, B) => Future[Z]): NJRetry[F, (A, B), Z] =
    retry((a: A, b: B) => F.fromFuture(F.delay(f(a, b))))

  def retryFuture[A, B, C, Z](f: (A, B, C) => Future[Z]): NJRetry[F, (A, B, C), Z] =
    retry((a: A, b: B, c: C) => F.fromFuture(F.delay(f(a, b, c))))

  def retryFuture[A, B, C, D, Z](f: (A, B, C, D) => Future[Z]): NJRetry[F, (A, B, C, D), Z] =
    retry((a: A, b: B, c: C, d: D) => F.fromFuture(F.delay(f(a, b, c, d))))

  def retryFuture[A, B, C, D, E, Z](f: (A, B, C, D, E) => Future[Z]): NJRetry[F, (A, B, C, D, E), Z] =
    retry((a: A, b: B, c: C, d: D, e: E) => F.fromFuture(F.delay(f(a, b, c, d, e))))

  def run[Z](fb: F[Z]): F[Z]                   = retry(fb).run
  def run[Z](sfb: Stream[F, Z]): F[Unit]       = run(sfb.compile.drain)
  def runFuture[Z](future: F[Future[Z]]): F[Z] = retryFuture(future).run

  override def put(fields: (String, TraceValue)*): F[Unit] = underlieSpan.put(fields*)
  override def kernel: F[Kernel]                           = underlieSpan.kernel
  override def span(name: String): Resource[F, NJSpan[F]] =
    underlieSpan.span(name).map(new NJSpan[F](_, name, metricRegistry, serviceStatus, channel, agentConfig))
  override def traceId: F[Option[String]] = underlieSpan.traceId
  override def spanId: F[Option[String]]  = underlieSpan.spanId
  override def traceUri: F[Option[URI]]   = underlieSpan.traceUri
}
