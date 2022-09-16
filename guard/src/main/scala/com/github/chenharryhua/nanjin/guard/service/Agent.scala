package com.github.chenharryhua.nanjin.guard.service

import cats.{Alternative, Endo, Traverse}
import cats.data.{Ior, IorT, Kleisli}
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.Name
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Json

import java.time.ZoneId
import scala.concurrent.Future
import scala.util.control.NonFatal

final class Agent[F[_]] private[service] (
  metricRegistry: MetricRegistry,
  serviceStatus: Ref[F, ServiceStatus],
  channel: Channel[F, NJEvent],
  agentConfig: AgentConfig)(implicit F: Async[F])
    extends UpdateConfig[AgentConfig, Agent[F]] {

  // parameters

  lazy val agentParams: AgentParams     = agentConfig.evalConfig
  lazy val serviceParams: ServiceParams = agentParams.serviceParams

  def zoneId: ZoneId = agentParams.serviceParams.taskParams.zoneId

  override def updateConfig(f: Endo[AgentConfig]): Agent[F] =
    new Agent[F](metricRegistry, serviceStatus, channel, f(agentConfig))

  def span(name: Name): Agent[F]     = updateConfig(_.withSpan(name))
  def span(ls: List[Name]): Agent[F] = updateConfig(_.withSpan(ls))

  def trivial: Agent[F]  = updateConfig(_.withLowImportance)
  def silent: Agent[F]   = updateConfig(_.withMediumImportance)
  def notice: Agent[F]   = updateConfig(_.withHighImportance)
  def critical: Agent[F] = updateConfig(_.withCriticalImportance)

  def expensive: Agent[F] = updateConfig(_.withExpensive(isCostly = true))
  def cheap: Agent[F]     = updateConfig(_.withExpensive(isCostly = false))

  // retries

  def retry[Z](fb: F[Z]): NJRetry0[F, Z] = // 0 arity
    new NJRetry0[F, Z](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      arrow = fb,
      transInput = F.pure(Json.Null),
      transOutput = _ => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, Z](f: A => F[Z]): NJRetry[F, A, Z] =
    new NJRetry[F, A, Z](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      arrow = f,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: A, _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, Z](f: (A, B) => F[Z]): NJRetry[F, (A, B), Z] =
    new NJRetry[F, (A, B), Z](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, Z](f: (A, B, C) => F[Z]): NJRetry[F, (A, B, C), Z] =
    new NJRetry[F, (A, B, C), Z](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, D, Z](f: (A, B, C, D) => F[Z]): NJRetry[F, (A, B, C, D), Z] =
    new NJRetry[F, (A, B, C, D), Z](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      arrow = f.tupled,
      transInput = _ => F.pure(Json.Null),
      transOutput = (_: (A, B, C, D), _: Z) => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex)))
    )

  def retry[A, B, C, D, E, Z](f: (A, B, C, D, E) => F[Z]): NJRetry[F, (A, B, C, D, E), Z] =
    new NJRetry[F, (A, B, C, D, E), Z](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
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

  // others

  def broker(brokerName: Name): NJBroker[F] =
    new NJBroker[F](
      digested = Digested(serviceParams, agentParams.spans.appended(brokerName)),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
      isError = false,
      isCounting = false
    )

  def alert(alertName: Name): NJAlert[F] =
    new NJAlert(
      digested = Digested(serviceParams, agentParams.spans.appended(alertName)),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
      isCounting = false
    )

  def counter(counterName: Name): NJCounter[F] =
    new NJCounter(
      digested = Digested(serviceParams, agentParams.spans.appended(counterName)),
      metricRegistry = metricRegistry,
      isError = false)

  def meter(meterName: Name): NJMeter[F] =
    new NJMeter[F](
      digested = Digested(serviceParams, agentParams.spans.appended(meterName)),
      metricRegistry = metricRegistry,
      isCounting = false)

  def histogram(histoName: Name): NJHistogram[F] =
    new NJHistogram[F](
      digested = Digested(serviceParams, agentParams.spans.appended(histoName)),
      metricRegistry = metricRegistry,
      isCounting = false
    )

  lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](
      new MetricEventPublisher[F](
        channel = channel,
        metricRegistry = metricRegistry,
        serviceStatus = serviceStatus))

  lazy val runtime: NJRuntimeInfo[F] = new NJRuntimeInfo[F](serviceStatus = serviceStatus)

  // for convenience

  def run[Z](fb: F[Z]): F[Z]                   = retry(fb).run
  def run[Z](sfb: Stream[F, Z]): F[Unit]       = run(sfb.compile.drain)
  def runFuture[Z](future: F[Future[Z]]): F[Z] = retryFuture(future).run

  def nonStop[B](fb: F[B]): F[Nothing] =
    updateConfig(
      _.withSpan(Name("nonStop")).withoutTiming.withoutCounting.withLowImportance
        .withExpensive(true)
        .withAlwaysGiveUp)
      .retry(fb)
      .run
      .flatMap[Nothing](_ => F.raiseError(ActionException.UnexpectedlyTerminated))

  def nonStop[B](sfb: Stream[F, B]): F[Nothing] = nonStop(sfb.compile.drain)

  def quasi[G[_]: Traverse: Alternative, B](tfb: G[F[B]]): IorT[F, G[Throwable], G[B]] =
    IorT(run(tfb.traverse(_.attempt).map(_.partitionEither(identity)).map { case (fail, succ) =>
      (fail.size, succ.size) match {
        case (0, _) => Ior.Right(succ)
        case (_, 0) => Ior.left(fail)
        case _      => Ior.Both(fail, succ)
      }
    }))

  def quasi[B](fbs: F[B]*): IorT[F, List[Throwable], List[B]] = quasi[List, B](fbs.toList)

  def quasi[G[_]: Traverse: Alternative, B](parallelism: Int, tfb: G[F[B]]): IorT[F, G[Throwable], G[B]] =
    IorT(run(F.parTraverseN(parallelism)(tfb)(_.attempt).map(_.partitionEither(identity)).map {
      case (fail, succ) =>
        (fail.size, succ.size) match {
          case (0, _) => Ior.Right(succ)
          case (_, 0) => Ior.left(fail)
          case _      => Ior.Both(fail, succ)
        }
    }))

  def quasi[B](parallelism: Int)(tfb: F[B]*): IorT[F, List[Throwable], List[B]] =
    quasi[List, B](parallelism, tfb.toList)
}
