package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Unique}
import cats.effect.std.{AtomicCell, Dispatcher}
import cats.syntax.flatMap.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.{awakeEvery, policies}
import cron4s.CronExpr
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import natchez.{EntryPoint, Kernel, Span}
import org.http4s.HttpRoutes
import org.typelevel.vault.{Key, Locker, Vault}
import retry.{RetryPolicies, RetryPolicy}

import java.time.{Instant, ZoneId, ZonedDateTime}

sealed trait Agent[F[_]] extends EntryPoint[F] {
  // trace
  def entryPoint: Resource[F, EntryPoint[F]]
  def traceServer(routes: Span[F] => HttpRoutes[F]): HttpRoutes[F]

  // date-time
  def zoneId: ZoneId
  def zonedNow: F[ZonedDateTime]
  def toZonedDateTime(ts: Instant): ZonedDateTime

  // metrics
  def metrics: NJMetrics[F]
  def action(name: String, f: Endo[ActionConfig] = identity): NJActionBuilder[F]
  def broker(brokerName: String): NJBroker[F]
  def alert(alertName: String): NJAlert[F]
  def counter(counterName: String): NJCounter[F]
  def meter(meterName: String): NJMeter[F]
  def histogram(histoName: String): NJHistogram[F]
  def gauge(gaugeName: String): NJGauge[F]

  // ticks
  def ticks(policy: RetryPolicy[F]): Stream[F, Int]
  def ticks(cronExpr: CronExpr, f: Endo[RetryPolicy[F]] = identity): Stream[F, Int]

}

final class GeneralAgent[F[_]] private[service] (
  val entryPoint: Resource[F, EntryPoint[F]],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  signallingMapRef: SignallingMapRef[F, Unique.Token, Option[Locker]],
  atomicCell: AtomicCell[F, Vault],
  dispatcher: Dispatcher[F])(implicit F: Async[F])
    extends Agent[F] {
  // trace
  override def root(name: String, options: Span.Options): Resource[F, Span[F]] =
    entryPoint.flatMap(_.root(name, options))

  override def continue(name: String, kernel: Kernel, options: Span.Options): Resource[F, Span[F]] =
    entryPoint.flatMap(_.continue(name, kernel, options))

  override def continueOrElseRoot(name: String, kernel: Kernel, options: Span.Options): Resource[F, Span[F]] =
    entryPoint.flatMap(_.continueOrElseRoot(name, kernel, options))

  override def traceServer(routes: Span[F] => HttpRoutes[F]): HttpRoutes[F] =
    HttpTrace.server[F](routes, entryPoint)

  // data time
  override val zoneId: ZoneId                              = serviceParams.taskParams.zoneId
  override val zonedNow: F[ZonedDateTime]                  = serviceParams.zonedNow[F]
  override def toZonedDateTime(ts: Instant): ZonedDateTime = serviceParams.toZonedDateTime(ts)

  // metrics
  override def action(name: String, f: Endo[ActionConfig] = identity): NJActionBuilder[F] =
    new NJActionBuilder[F](
      actionName = name,
      metricRegistry = metricRegistry,
      channel = channel,
      actionConfig = f(ActionConfig(serviceParams)),
      retryPolicy = RetryPolicies.alwaysGiveUp[F]
    )

  override def broker(brokerName: String): NJBroker[F] =
    new NJBroker[F](
      digested = Digested(serviceParams, brokerName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      isError = false,
      isCounting = false,
      dispatcher = dispatcher
    )

  override def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      digested = Digested(serviceParams, alertName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      isCounting = false,
      dispatcher = dispatcher
    )

  override def counter(counterName: String): NJCounter[F] =
    new NJCounter(digested = Digested(serviceParams, counterName), metricRegistry = metricRegistry)

  override def meter(meterName: String): NJMeter[F] =
    new NJMeter[F](
      digested = Digested(serviceParams, meterName),
      metricRegistry = metricRegistry,
      isCounting = false)

  override def histogram(histoName: String): NJHistogram[F] =
    new NJHistogram[F](
      digested = Digested(serviceParams, histoName),
      metricRegistry = metricRegistry,
      isCounting = false
    )

  override def gauge(gaugeName: String): NJGauge[F] =
    new NJGauge[F](Digested(serviceParams, gaugeName), metricRegistry, dispatcher)

  override lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](channel = channel, metricRegistry = metricRegistry, serviceParams = serviceParams)

  // ticks
  override def ticks(policy: RetryPolicy[F]): Stream[F, Int] = awakeEvery[F](policy)

  override def ticks(cronExpr: CronExpr, f: Endo[RetryPolicy[F]] = identity): Stream[F, Int] =
    awakeEvery[F](f(policies.cronBackoff[F](cronExpr, zoneId)))

  // general agent section, not in Agent API

  def signalBox[A](initValue: F[A]): NJSignalBox[F, A] = {
    val token = new Unique.Token
    val key   = new Key[A](token)
    new NJSignalBox[F, A](signallingMapRef(token), key, initValue)
  }
  def signalBox[A](initValue: => A): NJSignalBox[F, A] =
    signalBox(F.delay(initValue))

  def atomicBox[A](initValue: F[A]): NJAtomicBox[F, A] =
    new NJAtomicBox[F, A](atomicCell, new Key[A](new Unique.Token()), initValue)
  def atomicBox[A](initValue: => A): NJAtomicBox[F, A] =
    atomicBox[A](F.delay(initValue))

  def nonStop[A](sfa: Stream[F, A]): F[Nothing] =
    action("nonStop", _.withoutTiming.withoutCounting.trivial)
      .withRetryPolicy(RetryPolicies.alwaysGiveUp)
      .retry(sfa.compile.drain)
      .run
      .flatMap[Nothing](_ => F.raiseError(ActionException.UnexpectedlyTerminated))
}
