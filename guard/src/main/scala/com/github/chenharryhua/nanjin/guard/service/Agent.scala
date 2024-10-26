package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.metrics.NJMetrics
import fs2.Stream
import fs2.concurrent.Channel

import java.time.{Instant, ZoneId, ZonedDateTime}

sealed trait Agent[F[_]] {
  // date-time
  def zoneId: ZoneId
  def zonedNow: F[ZonedDateTime]
  def toZonedDateTime(ts: Instant): ZonedDateTime

  def withMeasurement(name: String): Agent[F]
  def enable(isEnabled: Boolean): Agent[F]

  // actions
  def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F]
  final def action(actionName: String): NJAction[F] = action(actionName, identity)

  def batch(name: String, f: Endo[NJAction.Builder]): NJBatch[F]
  final def batch(name: String): NJBatch[F] = batch(name, identity)

  def alert(alertName: String, f: Endo[NJAlert.Builder]): Resource[F, NJAlert[F]]
  final def alert(alertName: String): Resource[F, NJAlert[F]] = alert(alertName, identity)

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]

  // metrics
  def adhoc: NJMetricsReport[F]
  def metrics(name: String, f: Endo[NJMetrics.Builder]): NJMetrics[F]
  final def metrics(name: String): NJMetrics[F] = metrics(name, identity)
}

final private class GeneralAgent[F[_]: Async] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement,
  isEnabled: Boolean)
    extends Agent[F] {

  // date time
  override val zonedNow: F[ZonedDateTime] = serviceParams.zonedNow[F]
  override val zoneId: ZoneId             = serviceParams.zoneId

  override def toZonedDateTime(ts: Instant): ZonedDateTime = serviceParams.toZonedDateTime(ts)

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name), isEnabled)

  override def enable(isEnabled: Boolean): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, measurement, isEnabled)

  override def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F] =
    new NJAction[F](
      actionConfig = f(
        ActionConfig(
          isEnabled = isEnabled,
          actionName = ActionName(actionName),
          measurement = measurement,
          serviceParams = serviceParams
        )),
      metricRegistry = metricRegistry,
      channel = channel
    )

  override def batch(name: String, f: Endo[NJAction.Builder]): NJBatch[F] =
    new NJBatch[F](action(name, f), metrics(name, identity))

  override def alert(alertName: String, f: Endo[NJAlert.Builder]): Resource[F, NJAlert[F]] = {
    val init = new NJAlert.Builder(isEnabled, measurement, false)
    f(init).build[F](alertName, metricRegistry, channel, serviceParams)
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object adhoc extends NJMetricsReport[F](channel, serviceParams, metricRegistry)

  override def metrics(name: String, f: Endo[NJMetrics.Builder]): NJMetrics[F] = {
    val metricName = MetricName(serviceParams, measurement, name)
    f(new NJMetrics.Builder(isEnabled)).build[F](metricRegistry, metricName)
  }
}
