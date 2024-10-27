package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.Async
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

  // actions
  def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F]
  final def action(actionName: String): NJAction[F] = action(actionName, identity)

  def batch(name: String): NJBatch[F]

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]

  // metrics
  def adhoc: NJMetricsReport[F]

  def facilitator(name: String, f: Endo[NJFacilitator.Builder]): NJFacilitator[F]
  final def facilitator(name: String): NJFacilitator[F] = facilitator(name, identity)
}

final private class GeneralAgent[F[_]: Async] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement)
    extends Agent[F] {

  // date time
  override val zonedNow: F[ZonedDateTime] = serviceParams.zonedNow[F]
  override val zoneId: ZoneId             = serviceParams.zoneId

  override def toZonedDateTime(ts: Instant): ZonedDateTime = serviceParams.toZonedDateTime(ts)

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name))

  override def action(actionName: String, f: Endo[NJAction.Builder]): NJAction[F] =
    new NJAction[F](
      actionConfig = f(
        ActionConfig(
          isEnabled = true,
          actionName = ActionName(actionName),
          measurement = measurement,
          serviceParams = serviceParams
        )),
      metricRegistry = metricRegistry,
      channel = channel
    )

  override def batch(name: String): NJBatch[F] = {
    val metricName = MetricName(serviceParams, measurement, name)
    new NJBatch[F](NJMetrics(metricName, metricRegistry))
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object adhoc extends NJMetricsReport[F](channel, serviceParams, metricRegistry)

  override def facilitator(name: String, f: Endo[NJFacilitator.Builder]): NJFacilitator[F] = {
    val metricName = MetricName(serviceParams, measurement, name)
    f(new NJFacilitator.Builder(Policy.giveUp)).build[F](metricName, serviceParams, metricRegistry, channel)
  }
}
