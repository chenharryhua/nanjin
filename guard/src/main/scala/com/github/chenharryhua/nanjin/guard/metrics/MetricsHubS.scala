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

/** Stream-native interface for registering and using metrics.
  *
  * Obtain one with `Agent.metricsHubS(label)`. Each operation returns an `fs2.Stream` that acquires the
  * metric when the stream runs and unregisters it when the stream terminates, so registration can be composed
  * directly with other stream operations.
  *
  * For example:
  *
  * {{ agent.metricsHubS("requests").counter("total").flatMap { counter => Stream.repeatEval(counter.inc(1)) }
  * }}
  *
  * `MetricsHubS(hub)` is the bridge used when a resource-based `MetricsHub` is already available.
  */
sealed trait MetricsHubS[F[_]] {

  /** Metric label shared by instruments created from this hub. */
  def metricLabel: MetricLabel

  /** Register a counter and emit its effectful handle. */
  def counter(name: String, f: Endo[Counter.Builder] = identity): Stream[F, Counter[F]]

  /** Register a counter and emit its unsafe handle. */
  def unsafeCounter(name: String, f: Endo[Counter.Builder] = identity): Stream[F, UnsafeCounter]

  /** Register a meter and emit its effectful handle. */
  def meter(name: String, f: Endo[Meter.Builder] = identity): Stream[F, Meter[F]]

  /** Register a meter and emit its unsafe handle. */
  def unsafeMeter(name: String, f: Endo[Meter.Builder] = identity): Stream[F, UnsafeMeter]

  /** Register a histogram and emit its effectful handle. */
  def histogram(name: String, f: Endo[Histogram.Builder] = identity): Stream[F, Histogram[F]]

  /** Register a histogram and emit its unsafe handle. */
  def unsafeHistogram(name: String, f: Endo[Histogram.Builder] = identity): Stream[F, UnsafeHistogram]

  /** Register a timer and emit its effectful handle. */
  def timer(name: String, f: Endo[Timer.Builder] = identity): Stream[F, Timer[F]]

  /** Register a timer and emit its unsafe handle. */
  def unsafeTimer(name: String, f: Endo[Timer.Builder] = identity): Stream[F, UnsafeTimer]

  /** Register a custom gauge and emit unit when registration succeeds. */
  def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Stream[F, Unit]

  /** Register a health check and emit unit when registration succeeds. */
  def healthCheck(name: String, f: HealthCheck.Builder => HealthCheck.Registered[F]): Stream[F, Unit]

  /** Register a percentile gauge and emit its handle. */
  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Stream[F, Percentile[F]]

  /** Register an idle-time gauge and emit its handle. */
  def idleGauge(name: String, f: Endo[IdleGauge.Builder] = identity): Stream[F, IdleGauge[F]]

  /** Register an active-time gauge and emit its handle. */
  def activeGauge(name: String, f: Endo[ActiveGauge.Builder] = identity): Stream[F, ActiveGauge[F]]

  /** Register an STM-backed gauge and emit its transactional variable. */
  def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Stream[F, stm.TVar[A]]

  /** Register a two-sided balance gauge and emit its transfer handle. */
  def balanceGauge[A: {Group, Encoder}](
    source: (String, A),
    target: (String, A)): Stream[F, BalanceGauge[F, A]]
}

object MetricsHubS {

  /** Create the stream interface backed by a resource-based metrics hub. */
  def apply[F[_]](hub: MetricsHub[F])(using MonadCancel[F, Throwable]): MetricsHubS[F] =
    new MetricsHubS[F] {

      override val metricLabel: MetricLabel = hub.metricLabel

      override def counter(name: String, f: Endo[Counter.Builder]): Stream[F, Counter[F]] =
        Stream.resource(hub.counter(name, f))

      override def unsafeCounter(name: String, f: Endo[Counter.Builder]): Stream[F, UnsafeCounter] =
        Stream.resource(hub.unsafeCounter(name, f))

      override def meter(name: String, f: Endo[Meter.Builder]): Stream[F, Meter[F]] =
        Stream.resource(hub.meter(name, f))

      override def unsafeMeter(name: String, f: Endo[Meter.Builder]): Stream[F, UnsafeMeter] =
        Stream.resource(hub.unsafeMeter(name, f))

      override def histogram(name: String, f: Endo[Histogram.Builder]): Stream[F, Histogram[F]] =
        Stream.resource(hub.histogram(name, f))

      override def unsafeHistogram(name: String, f: Endo[Histogram.Builder]): Stream[F, UnsafeHistogram] =
        Stream.resource(hub.unsafeHistogram(name, f))

      override def timer(name: String, f: Endo[Timer.Builder]): Stream[F, Timer[F]] =
        Stream.resource(hub.timer(name, f))

      override def unsafeTimer(name: String, f: Endo[Timer.Builder]): Stream[F, UnsafeTimer] =
        Stream.resource(hub.unsafeTimer(name, f))

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
