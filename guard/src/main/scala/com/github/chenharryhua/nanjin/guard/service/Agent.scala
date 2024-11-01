package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Resource}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.metrics.NJMetrics
import fs2.Stream
import fs2.concurrent.Channel

import java.time.ZoneId

sealed trait Agent[F[_]] {
  def zoneId: ZoneId

  def withMeasurement(name: String): Agent[F]

  def batch(name: String): NJBatch[F]

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]

  // metrics
  def adhoc: NJMetricsReport[F]

  def herald: NJHerald[F]

  def metrics[A, B](name: String)(
    g: NJMetrics[F] => Resource[F, Kleisli[F, A, B]]): Resource[F, Kleisli[F, A, B]]

  def createRetry(policy: Policy): Resource[F, NJRetry[F]]
}

final private class GeneralAgent[F[_]: Async] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement)
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name))

  override def batch(name: String): NJBatch[F] = {
    val metricName = MetricName(serviceParams, measurement, name)
    new NJBatch[F](new NJMetrics.Impl[F](metricName, metricRegistry, isEnabled = true))
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object adhoc extends NJMetricsReport[F](channel, serviceParams, metricRegistry)

  override def createRetry(policy: Policy): Resource[F, NJRetry[F]] =
    Resource.pure(new NJRetry.Impl[F](serviceParams.initialStatus.renewPolicy(policy)))

  override def metrics[A, B](name: String)(
    f: NJMetrics[F] => Resource[F, Kleisli[F, A, B]]): Resource[F, Kleisli[F, A, B]] = {
    val metricName = MetricName(serviceParams, measurement, name)
    f(new NJMetrics.Impl[F](metricName, metricRegistry, isEnabled = true))
  }

  override val herald: NJHerald[F] =
    new NJHerald.Impl[F](serviceParams, channel)
}
