package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
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

  // actions
  def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F]
  final def action(actionName: String): NJAction[F] = action(actionName, identity)

  def batch(name: String, f: Endo[NJAction.Builder]): NJBatch[F]
  final def batch(name: String): NJBatch[F] = batch(name, identity)

  // health check
  def healthCheck(hcName: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F]
  final def healthCheck(hcName: String): NJHealthCheck[F] = healthCheck(hcName, identity)

  // metrics
  def jvmGauge: JvmGauge[F]

  def ratio(ratioName: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]]
  final def ratio(ratioName: String): Resource[F, NJRatio[F]] = ratio(ratioName, identity)

  def gauge(gaugeName: String, f: Endo[NJGauge.Builder]): NJGauge[F]
  final def gauge(gaugeName: String): NJGauge[F] = gauge(gaugeName, identity)

  def alert(alertName: String, f: Endo[NJAlert.Builder]): Resource[F, NJAlert[F]]
  final def alert(alertName: String): Resource[F, NJAlert[F]] = alert(alertName, identity)

  def counter(counterName: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]]
  final def counter(counterName: String): Resource[F, NJCounter[F]] = counter(counterName, identity)

  def meter(meterName: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]]
  final def meter(meterName: String): Resource[F, NJMeter[F]] = meter(meterName, identity)

  def histogram(histogramName: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]]
  final def histogram(histogramName: String): Resource[F, NJHistogram[F]] =
    histogram(histogramName, identity)

  def flowMeter(name: String, f: Endo[NJFlowMeter.Builder]): Resource[F, NJFlowMeter[F]]
  final def flowMeter(name: String): Resource[F, NJFlowMeter[F]] = flowMeter(name, identity)

  def timer(timerName: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]]
  final def timer(timerName: String): Resource[F, NJTimer[F]] = timer(timerName, identity)

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]
}

final private class GeneralAgent[F[_]: Async] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement)
    extends Agent[F] {

  // data time
  override val zonedNow: F[ZonedDateTime] = serviceParams.zonedNow[F]
  override val zoneId: ZoneId             = serviceParams.zoneId

  override def toZonedDateTime(ts: Instant): ZonedDateTime = serviceParams.toZonedDateTime(ts)

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name))

  private object builders {

    lazy val ratio: NJRatio.Builder =
      new NJRatio.Builder(measurement = measurement, translator = NJRatio.translator, isEnabled = true)

    lazy val gauge: NJGauge.Builder =
      new NJGauge.Builder(measurement = measurement, timeout = 5.seconds, isEnabled = true)

    lazy val healthCheck: NJHealthCheck.Builder =
      new NJHealthCheck.Builder(measurement = measurement, timeout = 5.seconds, isEnabled = true)

    lazy val timer: NJTimer.Builder =
      new NJTimer.Builder(measurement = measurement, isCounting = false, reservoir = None, isEnabled = true)

    lazy val flowerMeter: NJFlowMeter.Builder =
      new NJFlowMeter.Builder(
        measurement = measurement,
        unit = NJUnits.COUNT,
        isCounting = false,
        reservoir = None,
        isEnabled = true)

    lazy val histogram: NJHistogram.Builder =
      new NJHistogram.Builder(
        measurement = measurement,
        unit = NJUnits.COUNT,
        isCounting = false,
        reservoir = None,
        isEnabled = true)

    lazy val meter: NJMeter.Builder =
      new NJMeter.Builder(
        measurement = measurement,
        unit = NJUnits.COUNT,
        isCounting = false,
        isEnabled = true)

    lazy val counter: NJCounter.Builder =
      new NJCounter.Builder(measurement = measurement, isRisk = false, isEnabled = true)

    lazy val alert: NJAlert.Builder =
      new NJAlert.Builder(measurement = measurement, isCounting = false, isEnabled = true)
  }

  override def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F] =
    new NJAction[F](
      actionConfig = f(ActionConfig(ActionName(actionName), measurement, serviceParams)),
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

  override def flowMeter(name: String, f: Endo[NJFlowMeter.Builder]): Resource[F, NJFlowMeter[F]] =
    f(builders.flowerMeter).build[F](name, metricRegistry, serviceParams)

  override def timer(timerName: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]] =
    f(builders.timer).build[F](timerName, metricRegistry, serviceParams)

  override def gauge(gaugeName: String, f: Endo[NJGauge.Builder]): NJGauge[F] =
    f(builders.gauge).build[F](gaugeName, metricRegistry, serviceParams)

  override def healthCheck(hcName: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F] =
    f(builders.healthCheck).build[F](hcName, metricRegistry, serviceParams)

  override def ratio(ratioName: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]] =
    f(builders.ratio).build[F](ratioName, metricRegistry, serviceParams)

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object jvmGauge extends JvmGauge[F](metricRegistry)

  override object metrics extends NJMetrics[F](channel, serviceParams, metricRegistry)
}
