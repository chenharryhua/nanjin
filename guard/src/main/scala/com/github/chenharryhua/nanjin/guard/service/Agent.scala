package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Resource}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import fs2.Stream
import fs2.concurrent.Channel

import java.time.ZoneId

sealed trait Agent[F[_]] {
  def zoneId: ZoneId

  def withMeasurement(name: String): Agent[F]

  def batch(label: String): Batch[F]

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]
  final def ticks(f: Policy.type => Policy): Stream[F, Tick] =
    ticks(f(Policy))

  // metrics adhoc report
  def adhoc: MetricsReport[F]

  def herald: Herald[F]

  def facilitate[A](label: String)(f: Metrics[F] => A): A

  def createRetry(policy: Policy): Resource[F, Retry[F]]
  final def createRetry(f: Policy.type => Policy): Resource[F, Retry[F]] =
    createRetry(f(Policy))
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

  override def batch(label: String): Batch[F] = {
    val metricLabel = MetricLabel(serviceParams, measurement, label)
    new Batch[F](new Metrics.Impl[F](metricLabel, metricRegistry, isEnabled = true))
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object adhoc extends MetricsReport[F](channel, serviceParams, metricRegistry)

  override def createRetry(policy: Policy): Resource[F, Retry[F]] =
    Resource.pure(new Retry.Impl[F](serviceParams.initialStatus.renewPolicy(policy)))

  override def facilitate[A](label: String)(f: Metrics[F] => A): A = {
    val metricLabel = MetricLabel(serviceParams, measurement, label)
    f(new Metrics.Impl[F](metricLabel, metricRegistry, isEnabled = true))
  }

  override val herald: Herald[F] =
    new Herald.Impl[F](serviceParams, channel)
}
