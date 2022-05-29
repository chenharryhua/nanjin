package com.github.chenharryhua.nanjin.guard.service

import cats.{Alternative, Endo, Traverse}
import cats.data.{Ior, IorT, Kleisli}
import cats.effect.kernel.{Async, Ref}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.common.guard.{MaxRetry, Span}
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import eu.timepit.refined.refineMV
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

  private lazy val agentParams: AgentParams     = agentConfig.evalConfig
  private lazy val serviceParams: ServiceParams = agentParams.serviceParams

  def zoneId: ZoneId         = agentParams.serviceParams.taskParams.zoneId
  def digestedName: Digested = agentParams.serviceParams.digestSpans(agentParams.spans)

  override def updateConfig(f: Endo[AgentConfig]): Agent[F] =
    new Agent[F](metricRegistry, serviceStatus, channel, f(agentConfig))

  def span(name: Span): Agent[F] = updateConfig(_.withSpan(name))

  def trivial: Agent[F]  = updateConfig(_.withLowImportance)
  def normal: Agent[F]   = updateConfig(_.withMediumImportance)
  def notice: Agent[F]   = updateConfig(_.withHighImportance)
  def critical: Agent[F] = updateConfig(_.withCriticalImportance)

  def expensive: Agent[F] = updateConfig(_.withExpensive(isCostly = true))
  def cheap: Agent[F]     = updateConfig(_.withExpensive(isCostly = false))

  def retry[A, B](f: A => F[B]): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      afb = f,
      transInput = _ => F.pure(Json.Null),
      transOutput = _ => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex))))

  def retry[B](fb: F[B]): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      serviceStatus = serviceStatus,
      metricRegistry = metricRegistry,
      channel = channel,
      actionParams = ActionParams(agentParams),
      fb = fb,
      transInput = F.pure(Json.Null),
      transOutput = _ => F.pure(Json.Null),
      isWorthRetry = Kleisli(ex => F.pure(NonFatal(ex))))

  def run[B](fb: F[B]): F[B]             = retry(fb).run
  def run[B](sfb: Stream[F, B]): F[Unit] = run(sfb.compile.drain)

  def retryFuture[A, B](f: A => Future[B]): NJRetry[F, A, B]  = retry((a: A) => F.fromFuture(F.delay(f(a))))
  def retryFuture[B](future: F[Future[B]]): NJRetryUnit[F, B] = retry(F.fromFuture(future))
  def runFuture[B](future: F[Future[B]]): F[B]                = retryFuture(future).run

  def broker(brokerName: Span): NJBroker[F] =
    new NJBroker[F](
      digested = serviceParams.digestSpans(agentParams.spans :+ brokerName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
      isError = false,
      isCounting = false)

  def alert(alertName: Span): NJAlert[F] =
    new NJAlert(
      digested = serviceParams.digestSpans(agentParams.spans :+ alertName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
      isCounting = false)

  def counter(counterName: Span): NJCounter[F] =
    new NJCounter(
      digested = serviceParams.digestSpans(agentParams.spans :+ counterName),
      metricRegistry = metricRegistry,
      isError = false)

  def meter(meterName: Span): NJMeter[F] =
    new NJMeter[F](
      digested = serviceParams.digestSpans(agentParams.spans :+ meterName),
      metricRegistry = metricRegistry,
      isCounting = false)

  def histogram(histoName: Span): NJHistogram[F] =
    new NJHistogram[F](
      digested = serviceParams.digestSpans(agentParams.spans :+ histoName),
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

  // maximum retries
  def max(retries: MaxRetry): Agent[F] = updateConfig(_.withMaxRetries(retries))

  def nonStop[B](fb: F[B]): F[Nothing] =
    updateConfig(
      _.withSpan(Span("nonStop")).withoutTiming.withoutCounting.withLowImportance
        .withExpensive(true)
        .withMaxRetries(refineMV(0)))
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
