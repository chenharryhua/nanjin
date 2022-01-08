package com.github.chenharryhua.nanjin.guard.service

import cats.data.{Kleisli, Reader}
import cats.effect.kernel.{Async, Ref, RefSource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import cats.{Alternative, Traverse}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.UpdateConfig
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream
import fs2.concurrent.Channel

import java.time.ZoneId

final class Agent[F[_]] private[service] (
  metricRegistry: MetricRegistry,
  serviceStatus: RefSource[F, ServiceStatus],
  channel: Channel[F, NJEvent],
  ongoings: Ref[F, Set[ActionInfo]],
  dispatcher: Dispatcher[F],
  lastCounters: Ref[F, MetricSnapshot.LastCounters],
  agentConfig: AgentConfig)(implicit F: Async[F])
    extends UpdateConfig[AgentConfig, Agent[F]] {

  val agentParams: AgentParams     = agentConfig.evalConfig
  val serviceParams: ServiceParams = agentParams.serviceParams
  val zoneId: ZoneId               = agentParams.serviceParams.taskParams.zoneId
  val digestedName: DigestedName   = DigestedName(agentParams.spans, agentParams.serviceParams)

  override def updateConfig(f: AgentConfig => AgentConfig): Agent[F] =
    new Agent[F](metricRegistry, serviceStatus, channel, ongoings, dispatcher, lastCounters, f(agentConfig))

  def span(name: String): Agent[F] = updateConfig(_.withSpan(name))

  def trivial: Agent[F]  = updateConfig(_.withLowImportance)
  def normal: Agent[F]   = updateConfig(_.withMediumImportance)
  def notice: Agent[F]   = updateConfig(_.withHighImportance)
  def critical: Agent[F] = updateConfig(_.withCriticalImportance)

  def expensive: Agent[F] = updateConfig(_.withExpensive(isCostly = true))
  def cheap: Agent[F]     = updateConfig(_.withExpensive(isCostly = false))

  def retry[A, B](f: A => F[B]): NJRetry[F, A, B] =
    new NJRetry[F, A, B](
      metricRegistry = metricRegistry,
      channel = channel,
      ongoings = ongoings,
      actionParams = ActionParams(agentParams),
      kfab = Kleisli(f),
      succ = None,
      fail = None,
      isWorthRetry = Reader(_ => true))

  def retry[B](fb: F[B]): NJRetryUnit[F, B] =
    new NJRetryUnit[F, B](
      metricRegistry = metricRegistry,
      channel = channel,
      ongoings = ongoings,
      actionParams = ActionParams(agentParams),
      fb = fb,
      succ = None,
      fail = None,
      isWorthRetry = Reader(_ => true))

  def run[B](fb: F[B]): F[B]             = retry(fb).run
  def run[B](sfb: Stream[F, B]): F[Unit] = run(sfb.compile.drain)

  def broker(brokerName: String): NJBroker[F] =
    new NJBroker[F](
      metricName = DigestedName(agentParams.spans :+ brokerName, serviceParams),
      dispatcher = dispatcher,
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams,
      isCountAsError = false)

  def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      metricName = DigestedName(agentParams.spans :+ alertName, serviceParams),
      dispatcher = dispatcher,
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = agentParams.serviceParams)

  def counter(counterName: String): NJCounter[F] =
    new NJCounter(
      metricName = DigestedName(agentParams.spans :+ counterName, serviceParams),
      metricRegistry = metricRegistry,
      isCountAsError = false)

  def meter(meterName: String): NJMeter[F] =
    new NJMeter[F](
      metricName = DigestedName(agentParams.spans :+ meterName, serviceParams),
      metricRegistry = metricRegistry)

  def histogram(histoName: String): NJHistogram[F] =
    new NJHistogram[F](
      metricName = DigestedName(agentParams.spans :+ histoName, serviceParams),
      metricRegistry = metricRegistry
    )

  lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](
      new MetricEventPublisher[F](
        serviceParams = serviceParams,
        channel = channel,
        metricRegistry = metricRegistry,
        serviceStatus = serviceStatus,
        ongoings = ongoings,
        lastCounters = lastCounters),
      dispatcher = dispatcher)

  lazy val runtime: NJRuntimeInfo[F] =
    new NJRuntimeInfo[F](serviceParams = serviceParams, serviceStatus = serviceStatus, ongoings = ongoings)

  // maximum retries
  def max(retries: Int): Agent[F] = updateConfig(_.withMaxRetries(retries))

  def nonStop[B](fb: F[B]): F[Nothing] =
    span("nonStop")
      .max(retries = 0)
      .cheap
      .updateConfig(_.withoutTiming.withoutCounting.withLowImportance)
      .retry(fb)
      .run
      .flatMap[Nothing](_ => F.raiseError(ActionException.UnexpectedlyTerminated))

  def nonStop[B](sfb: Stream[F, B]): F[Nothing] = nonStop(sfb.compile.drain)

  def quasi[T[_]: Traverse: Alternative, B](tfb: T[F[B]]): F[T[B]] =
    run(tfb.traverse(_.attempt).map(_.partitionEither(identity)).map(_._2))

  def quasi[B](fbs: F[B]*): F[List[B]] = quasi[List, B](fbs.toList)

  def quasi[T[_]: Traverse: Alternative, B](parallelism: Int, tfb: T[F[B]]): F[T[B]] =
    run(F.parTraverseN(parallelism)(tfb)(_.attempt).map(_.partitionEither(identity)).map(_._2))

  def quasi[B](parallelism: Int)(tfb: F[B]*): F[List[B]] = quasi[List, B](parallelism, tfb.toList)
}
