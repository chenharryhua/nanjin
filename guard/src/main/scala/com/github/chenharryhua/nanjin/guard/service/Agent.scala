package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.Resource
import cats.effect.kernel.{Async, Unique}
import cats.effect.std.{AtomicCell, Dispatcher}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.policies
import cron4s.CronExpr
import fs2.Stream
import fs2.concurrent.{Channel, SignallingMapRef}
import fs2.io.net.Network
import natchez.{EntryPoint, Kernel, Span}
import org.http4s.HttpRoutes
import org.typelevel.vault.{Key, Locker, Vault}
import retry.{RetryPolicies, RetryPolicy}
import software.amazon.awssdk.services.cloudwatch.model.StandardUnit

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
  def withMeasurement(measurement: String): Agent[F]
  def metrics: NJMetrics[F]
  def action(name: String, f: Endo[ActionConfig]): NJActionBuilder[F]
  def action(name: String): NJActionBuilder[F]
  def alert(alertName: String): NJAlert[F]
  def counter(counterName: String): NJCounter[F]
  def meter(meterName: String, unitOfMeasure: StandardUnit): NJMeter[F]
  def histogram(histoName: String, unitOfMeasure: StandardUnit): NJHistogram[F]
  def gauge(gaugeName: String): NJGauge[F]

  // ticks
  def ticks(policy: RetryPolicy[F]): Stream[F, Tick]
  def ticks(cronExpr: CronExpr, f: Endo[RetryPolicy[F]]): Stream[F, Tick]
  def ticks(cronExpr: CronExpr): Stream[F, Tick]

  // udp
  def udpClient(name: String): NJUdpClient[F]
}

final class GeneralAgent[F[_]: Network] private[service] (
  val entryPoint: Resource[F, EntryPoint[F]],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  signallingMapRef: SignallingMapRef[F, Unique.Token, Option[Locker]],
  atomicCell: AtomicCell[F, Vault],
  dispatcher: Dispatcher[F],
  measurement: Measurement)(implicit F: Async[F])
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
  override def withMeasurement(measurement: String): Agent[F] =
    new GeneralAgent[F](
      entryPoint,
      serviceParams,
      metricRegistry,
      channel,
      signallingMapRef,
      atomicCell,
      dispatcher,
      measurement = Measurement(measurement)
    )

  override def action(name: String, f: Endo[ActionConfig]): NJActionBuilder[F] =
    new NJActionBuilder[F](
      actionName = name,
      measurement = measurement,
      metricRegistry = metricRegistry,
      channel = channel,
      actionConfig = f(ActionConfig(serviceParams)),
      retryPolicy = RetryPolicies.alwaysGiveUp[F]
    )
  override def action(name: String): NJActionBuilder[F] = action(name, identity)

  override def alert(alertName: String): NJAlert[F] =
    new NJAlert(
      name = MetricName(serviceParams, measurement, alertName),
      metricRegistry = metricRegistry,
      channel = channel,
      serviceParams = serviceParams,
      dispatcher = dispatcher,
      isCounting = false
    )

  override def counter(counterName: String): NJCounter[F] =
    new NJCounter(
      name = MetricName(serviceParams, measurement, counterName),
      metricRegistry = metricRegistry,
      isRisk = false)

  override def meter(meterName: String, unitOfMeasure: StandardUnit): NJMeter[F] =
    new NJMeter[F](
      name = MetricName(serviceParams, measurement, meterName),
      metricRegistry = metricRegistry,
      unit = unitOfMeasure,
      isCounting = false
    )

  override def histogram(histoName: String, unitOfMeasure: StandardUnit): NJHistogram[F] =
    new NJHistogram[F](
      name = MetricName(serviceParams, measurement, histoName),
      unit = unitOfMeasure,
      metricRegistry = metricRegistry,
      isCounting = false
    )

  override def gauge(gaugeName: String): NJGauge[F] =
    new NJGauge[F](
      name = MetricName(serviceParams, measurement, gaugeName),
      metricRegistry = metricRegistry,
      dispatcher = dispatcher
    )

  override lazy val metrics: NJMetrics[F] =
    new NJMetrics[F](channel = channel, metricRegistry = metricRegistry, serviceParams = serviceParams)

  // ticks
  override def ticks(policy: RetryPolicy[F]): Stream[F, Tick] = awakeEvery(policy)
  override def ticks(cronExpr: CronExpr, f: Endo[RetryPolicy[F]]): Stream[F, Tick] =
    awakeEvery(f(policies.cronBackoff[F](cronExpr, zoneId)))
  override def ticks(cronExpr: CronExpr): Stream[F, Tick] = ticks(cronExpr, identity)

  override def udpClient(name: String): NJUdpClient[F] =
    new NJUdpClient[F](
      name = MetricName(serviceParams, measurement, name),
      metricRegistry = metricRegistry,
      isCounting = false,
      isHistogram = false)

  // general agent section, not in Agent API

  def signalBox[A](initValue: F[A]): NJSignalBox[F, A] = {
    val token = new Unique.Token
    val key   = new Key[A](token)
    new NJSignalBox[F, A](signallingMapRef(token), key, initValue)
  }
  def signalBox[A](initValue: => A): NJSignalBox[F, A] = signalBox(F.delay(initValue))

  def atomicBox[A](initValue: F[A]): NJAtomicBox[F, A] =
    new NJAtomicBox[F, A](atomicCell, new Key[A](new Unique.Token), initValue)
  def atomicBox[A](initValue: => A): NJAtomicBox[F, A] = atomicBox[A](F.delay(initValue))

}
