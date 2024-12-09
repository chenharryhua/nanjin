package com.github.chenharryhua.nanjin.guard.service

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.implicits.catsSyntaxApplicativeId
import com.codahale.metrics.MetricRegistry
import com.github.benmanes.caffeine.cache.Cache
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

  /** start from first tick
    */
  def ticks(policy: Policy): Stream[F, Tick]
  final def ticks(f: Policy.type => Policy): Stream[F, Tick] =
    ticks(f(Policy))

  /** start from zeroth tick immediately
    */
  def tickImmediately(policy: Policy): Stream[F, Tick]
  final def tickImmediately(f: Policy.type => Policy): Stream[F, Tick] =
    tickImmediately(f(Policy))

  // metrics adhoc report
  def adhoc: MetricsReport[F]

  def herald: Herald[F]

  def facilitate[A](label: String)(f: Metrics[F] => A): A

  def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]]

  def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]]

  def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]]

}

final private class GeneralAgent[F[_]: Async](
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, Event],
  measurement: Measurement)
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name))

  override def batch(label: String): Batch[F] = {
    val metricLabel = MetricLabel(label, measurement)
    new Batch[F](new Metrics.Impl[F](metricLabel, metricRegistry))
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream.fromOne[F](policy, zoneId)

  override def tickImmediately(policy: Policy): Stream[F, Tick] =
    tickStream.fromZero(policy, zoneId)

  override def facilitate[A](label: String)(f: Metrics[F] => A): A = {
    val metricLabel = MetricLabel(label, measurement)
    f(new Metrics.Impl[F](metricLabel, metricRegistry))
  }

  override def circuitBreaker(f: Endo[CircuitBreaker.Builder]): Resource[F, CircuitBreaker[F]] =
    f(new CircuitBreaker.Builder(maxFailures = 5, policy = Policy.giveUp)).build[F](zoneId)

  override def caffeineCache[K, V](cache: Cache[K, V]): Resource[F, CaffeineCache[F, K, V]] =
    CaffeineCache.buildCache[F, K, V](cache)

  override def retry(f: Endo[Retry.Builder[F]]): Resource[F, Retry[F]] =
    f(new Retry.Builder[F](Policy.giveUp, _ => true.pure[F])).build(zoneId)

  override object adhoc extends MetricsReport[F](channel, serviceParams, metricRegistry)
  override object herald extends Herald.Impl[F](serviceParams, channel)
}
