package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.data.Kleisli
import cats.effect.kernel.{Async, Resource}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.config.CategoryKind.*
import com.github.chenharryhua.nanjin.guard.event.*
import fs2.Stream
import fs2.concurrent.Channel

import java.time.{Instant, ZoneId, ZonedDateTime}
import scala.concurrent.duration.DurationInt

sealed trait Agent[F[_]] {
  // date-time
  def zoneId: ZoneId
  def zonedNow: F[ZonedDateTime]
  def toZonedDateTime(ts: Instant): ZonedDateTime

  // metrics
  def metrics: NJMetrics[F]
  def withMeasurement(name: String): Agent[F]
  def enable(value: Boolean): Agent[F]

  // actions
  def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F]
  final def action(actionName: String): NJAction[F] = action(actionName, identity)

  def batch(name: String, f: Endo[NJAction.Builder]): NJBatch[F]
  final def batch(name: String): NJBatch[F] = batch(name, identity)

  // metrics
  def ratio(ratioName: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]]
  final def ratio(ratioName: String): Resource[F, NJRatio[F]] = ratio(ratioName, identity)

  def alert(alertName: String, f: Endo[NJAlert.Builder]): Resource[F, NJAlert[F]]
  final def alert(alertName: String): Resource[F, NJAlert[F]] = alert(alertName, identity)

  def counter(counterName: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]]
  final def counter(counterName: String): Resource[F, NJCounter[F]] = counter(counterName, identity)

  def meter(meterName: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]]
  final def meter(meterName: String): Resource[F, NJMeter[F]] = meter(meterName, identity)

  def histogram(histogramName: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]]
  final def histogram(histogramName: String): Resource[F, NJHistogram[F]] =
    histogram(histogramName, identity)

  def timer(timerName: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]]
  final def timer(timerName: String): Resource[F, NJTimer[F]] = timer(timerName, identity)

  // gauge
  def gauge(gaugeName: String, f: Endo[NJGauge.Builder]): NJGauge[F]
  final def gauge(gaugeName: String): NJGauge[F] = gauge(gaugeName, identity)

  def idleGauge(gaugeName: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Unit, Unit]]

  final def idleGauge(gaugeName: String): Resource[F, Kleisli[F, Unit, Unit]] =
    idleGauge(gaugeName, identity)

  def activeGauge(gaugeName: String, f: Endo[NJGauge.Builder]): Resource[F, Unit]
  final def activeGauge(gaugeName: String): Resource[F, Unit] = activeGauge(gaugeName, identity)

  // health check
  def healthCheck(hcName: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F]
  final def healthCheck(hcName: String): NJHealthCheck[F] = healthCheck(hcName, identity)

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]
}

final private class GeneralAgent[F[_]: Async] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement,
  isEnabled: Boolean)
    extends Agent[F] {

  private val F = Async[F]

  // date time
  override val zonedNow: F[ZonedDateTime] = serviceParams.zonedNow[F]
  override val zoneId: ZoneId             = serviceParams.zoneId

  override def toZonedDateTime(ts: Instant): ZonedDateTime = serviceParams.toZonedDateTime(ts)

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name), isEnabled)

  override def enable(value: Boolean): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, measurement, value)

  private object builders {

    lazy val ratio: NJRatio.Builder =
      new NJRatio.Builder(
        measurement = measurement,
        translator = NJRatio.translator,
        isEnabled = isEnabled,
        tag = MetricTag("ratio"))

    lazy val gauge: NJGauge.Builder =
      new NJGauge.Builder(
        measurement = measurement,
        timeout = 5.seconds,
        isEnabled = isEnabled,
        tag = MetricTag(GaugeKind.Gauge.entryName))

    lazy val healthCheck: NJHealthCheck.Builder =
      new NJHealthCheck.Builder(
        measurement = measurement,
        timeout = 5.seconds,
        isEnabled = isEnabled,
        tag = MetricTag(GaugeKind.HealthCheck.entryName))

    lazy val counter: NJCounter.Builder =
      new NJCounter.Builder(
        measurement = measurement,
        isRisk = false,
        isEnabled = isEnabled,
        tag = MetricTag(CounterKind.Counter.entryName))

    lazy val timer: NJTimer.Builder =
      new NJTimer.Builder(
        measurement = measurement,
        reservoir = None,
        isEnabled = isEnabled,
        tag = MetricTag(TimerKind.Timer.entryName))

    lazy val histogram: NJHistogram.Builder =
      new NJHistogram.Builder(
        measurement = measurement,
        unit = NJUnits.COUNT,
        reservoir = None,
        isEnabled = isEnabled,
        tag = MetricTag(HistogramKind.Histogram.entryName))

    lazy val meter: NJMeter.Builder =
      new NJMeter.Builder(
        measurement = measurement,
        unit = NJUnits.COUNT,
        isEnabled = isEnabled,
        tag = MetricTag(MeterKind.Meter.entryName))

    lazy val alert: NJAlert.Builder =
      new NJAlert.Builder(measurement = measurement, isCounting = false, isEnabled = isEnabled)
  }

  override def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F] =
    new NJAction[F](
      actionConfig = f(
        ActionConfig(
          actionName = ActionName(actionName),
          measurement = measurement,
          serviceParams = serviceParams,
          isEnabled = isEnabled)),
      metricRegistry = metricRegistry,
      channel = channel
    )

  override def batch(name: String, f: Endo[NJAction.Builder]): NJBatch[F] =
    new NJBatch[F](
      serviceParams = serviceParams,
      metricRegistry = metricRegistry,
      action = action(name, f),
      ratioBuilder = builders.ratio,
      gaugeBuilder = builders.gauge
    )

  override def alert(alertName: String, f: Endo[NJAlert.Builder]): Resource[F, NJAlert[F]] =
    f(builders.alert).build[F](alertName, metricRegistry, channel, serviceParams)

  override def counter(counterName: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]] =
    f(builders.counter).build[F](counterName, metricRegistry, serviceParams)

  override def meter(meterName: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]] =
    f(builders.meter).build[F](meterName, metricRegistry, serviceParams)

  override def histogram(histogramName: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]] =
    f(builders.histogram).build[F](histogramName, metricRegistry, serviceParams)

  override def timer(timerName: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]] =
    f(builders.timer).build[F](timerName, metricRegistry, serviceParams)

  override def gauge(gaugeName: String, f: Endo[NJGauge.Builder]): NJGauge[F] =
    f(builders.gauge).build[F](gaugeName, metricRegistry, serviceParams)

  override def idleGauge(gaugeName: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Unit, Unit]] =
    for {
      lastUpdate <- Resource.eval(F.monotonic.flatMap(F.ref))
      _ <- gauge(gaugeName, g => f(g.withTag("idle"))).register(
        for {
          pre <- lastUpdate.get
          now <- F.monotonic
        } yield DurationFormatter.defaultFormatter.format(now - pre)
      )
    } yield Kleisli[F, Unit, Unit](_ => F.monotonic.flatMap(lastUpdate.set))

  override def activeGauge(gaugeName: String, f: Endo[NJGauge.Builder]): Resource[F, Unit] =
    for {
      kickoff <- Resource.eval(F.monotonic)
      _ <- gauge(gaugeName, g => f(g.withTag("active"))).register(F.monotonic.map(now =>
        DurationFormatter.defaultFormatter.format(now - kickoff)))
    } yield ()

  override def healthCheck(hcName: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F] =
    f(builders.healthCheck).build[F](hcName, metricRegistry, serviceParams)

  override def ratio(ratioName: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]] =
    f(builders.ratio).build[F](ratioName, metricRegistry, serviceParams)

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object metrics extends NJMetrics[F](channel, serviceParams, metricRegistry)
}
