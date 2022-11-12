package com.github.chenharryhua.nanjin.guard.service

import cats.{Endo, Eval}
import cats.effect.kernel.Async
import cats.effect.Resource
import cats.implicits.toFlatMapOps
import com.codahale.metrics.{Gauge, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.{awakeEvery, policies}
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import cron4s.CronExpr
import fs2.concurrent.Channel
import fs2.Stream
import natchez.{EntryPoint, Kernel, Span}
import retry.{RetryPolicies, RetryPolicy}

import java.time.{ZoneId, ZonedDateTime}

final class Agent[F[_]] private[service] (
  val serviceParams: ServiceParams,
  val metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  entryPoint: Resource[F, EntryPoint[F]])(implicit F: Async[F])
    extends EntryPoint[F] {

  override def root(name: String): Resource[F, Span[F]] =
    entryPoint.flatMap(_.root(name))

  override def continue(name: String, kernel: Kernel): Resource[F, Span[F]] =
    entryPoint.flatMap(_.continue(name, kernel))

  override def continueOrElseRoot(name: String, kernel: Kernel): Resource[F, Span[F]] =
    entryPoint.flatMap(_.continueOrElseRoot(name, kernel))

  val zoneId: ZoneId = serviceParams.taskParams.zoneId

  val zonedNow: F[ZonedDateTime] = serviceParams.zonedNow[F]

  def action(name: String, cfg: Endo[ActionConfig] = identity): NJActionBuilder[F] =
    new NJActionBuilder[F](
      actionName = name,
      serviceParams = serviceParams,
      metricRegistry = metricRegistry,
      channel = channel,
      actionConfig = cfg(ActionConfig()),
      retryPolicy = RetryPolicies.alwaysGiveUp[F]
    )

  def broker(brokerName: String): NJBroker[F] =
    new NJBroker[F](
      digested = Digested(serviceParams, brokerName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      isError = false,
      isCounting = false
    )

  def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      digested = Digested(serviceParams, alertName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      isCounting = false
    )

  def counter(counterName: String): NJCounter[F] =
    new NJCounter(digested = Digested(serviceParams, counterName), metricRegistry = metricRegistry)

  def meter(meterName: String): NJMeter[F] =
    new NJMeter[F](
      digested = Digested(serviceParams, meterName),
      metricRegistry = metricRegistry,
      isCounting = false)

  def histogram(histoName: String): NJHistogram[F] =
    new NJHistogram[F](
      digested = Digested(serviceParams, histoName),
      metricRegistry = metricRegistry,
      isCounting = false
    )

  def gauge[A](gaugeName: String, value: Eval[A]): Gauge[A] =
    metricRegistry.register(
      "gauge." + Digested(serviceParams, gaugeName).metricRepr,
      new Gauge[A] { override def getValue: A = value.value })

  def gauge[A](gaugeName: String, value: => A): Gauge[A] =
    gauge[A](gaugeName, Eval.always(value))

  lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](channel = channel, metricRegistry = metricRegistry, serviceParams = serviceParams)

  def ticks(policy: RetryPolicy[F]): Stream[F, Int] = awakeEvery[F](policy)

  def ticks(cronExpr: CronExpr, f: Endo[RetryPolicy[F]] = identity): Stream[F, Int] =
    awakeEvery[F](f(policies.cronBackoff[F](cronExpr, zoneId)))

  // for convenience

  def nonStop[A](sfa: Stream[F, A]): F[Nothing] =
    action("nonStop", _.withoutTiming.withoutCounting.trivial)
      .withRetryPolicy(RetryPolicies.alwaysGiveUp)
      .retry(sfa.compile.drain)
      .run
      .flatMap[Nothing](_ => F.raiseError(ActionException.UnexpectedlyTerminated))
}
