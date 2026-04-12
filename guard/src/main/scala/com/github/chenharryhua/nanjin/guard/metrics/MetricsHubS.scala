package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.MonadCancel
import cats.kernel.Group
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import com.github.chenharryhua.nanjin.guard.metrics.gauges.{
  ActiveGauge,
  BalanceGauge,
  Gauge,
  HealthCheck,
  IdleGauge,
  Percentile
}
import fs2.Stream
import io.circe.Encoder
import io.github.timwspence.cats.stm.STM

trait MetricsHubS[F[_]] {
  def metricLabel: MetricLabel
  def counter(name: String, f: Endo[Counter.Builder] = identity): Stream[F, Counter[F]]
  def meter(name: String, f: Endo[Meter.Builder] = identity): Stream[F, Meter[F]]
  def histogram(name: String, f: Endo[Histogram.Builder] = identity): Stream[F, Histogram[F]]
  def timer(name: String, f: Endo[Timer.Builder] = identity): Stream[F, Timer[F]]
  // gauges
  def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Stream[F, Unit]
  def healthCheck(name: String, f: HealthCheck.Builder => HealthCheck.Registered[F]): Stream[F, Unit]
  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Stream[F, Percentile[F]]
  def idleGauge(name: String, f: Endo[IdleGauge.Builder] = identity): Stream[F, IdleGauge[F]]
  def activeGauge(name: String, f: Endo[ActiveGauge.Builder] = identity): Stream[F, ActiveGauge[F]]
  def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Stream[F, stm.TVar[A]]
  def balanceGauge[A: {Group, Encoder}](
    source: (String, A),
    target: (String, A)): Stream[F, BalanceGauge[F, A]]
}

object MetricsHubS {
  def apply[F[_]](hub: MetricsHub[F])(using MonadCancel[F, Throwable]): MetricsHubS[F] =
    new MetricsHubS[F] {

      override def metricLabel: MetricLabel = hub.metricLabel

      override def counter(name: String, f: Endo[Counter.Builder]): Stream[F, Counter[F]] =
        Stream.resource(hub.counter(name, f))

      override def meter(name: String, f: Endo[Meter.Builder]): Stream[F, Meter[F]] =
        Stream.resource(hub.meter(name, f))

      override def histogram(name: String, f: Endo[Histogram.Builder]): Stream[F, Histogram[F]] =
        Stream.resource(hub.histogram(name, f))

      override def timer(name: String, f: Endo[Timer.Builder]): Stream[F, Timer[F]] =
        Stream.resource(hub.timer(name, f))

      override def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Stream[F, Unit] =
        Stream.resource(hub.gauge(name, f))

      override def healthCheck(
        name: String,
        f: HealthCheck.Builder => HealthCheck.Registered[F]): Stream[F, Unit] =
        Stream.resource(hub.healthCheck(name, f))

      override def percentile(name: String, f: Endo[Percentile.Builder]): Stream[F, Percentile[F]] =
        Stream.resource(hub.percentile(name, f))

      override def idleGauge(name: String, f: Endo[IdleGauge.Builder]): Stream[F, IdleGauge[F]] =
        Stream.resource(hub.idleGauge(name, f))

      override def activeGauge(name: String, f: Endo[ActiveGauge.Builder]): Stream[F, ActiveGauge[F]] =
        Stream.resource(hub.activeGauge(name, f))

      override def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Stream[F, stm.TVar[A]] =
        Stream.resource(hub.txnGauge(stm, initial)(name))

      override def balanceGauge[A: {Group, Encoder}](
        source: (String, A),
        target: (String, A)): Stream[F, BalanceGauge[F, A]] =
        Stream.resource(hub.balanceGauge(source, target))
    }
}
