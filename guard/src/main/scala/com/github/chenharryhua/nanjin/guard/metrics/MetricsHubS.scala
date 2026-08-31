package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.MonadCancel
import cats.kernel.Group
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.{
  ActiveGauge,
  BalanceGauge,
  FrequencyCounter,
  Gauge,
  HealthCheck,
  IdleGauge,
  NumericGauge,
  Percentile
}
import com.github.chenharryhua.nanjin.guard.metrics.api.{Counter, Histogram, Meter, Timer}
import fs2.Stream
import io.circe.Encoder

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

  /** Metric scope shared by instruments created from this hub. */
  def scope: MetricScope

  /** Register a counter and emit its effectful handle. */
  def counter(name: String, f: Endo[Counter.Builder] = identity): Stream[F, Counter[F]]

  /** Register a meter and emit its effectful handle. */
  def meter(name: String, f: Endo[Meter.Builder] = identity): Stream[F, Meter[F]]

  /** Register a histogram and emit its effectful handle. */
  def histogram(name: String, f: Endo[Histogram.Builder] = identity): Stream[F, Histogram[F]]

  /** Register a timer and emit its effectful handle. */
  def timer(name: String, f: Endo[Timer.Builder] = identity): Stream[F, Timer[F]]

  /** Register a custom gauge and emit unit when registration succeeds. */
  def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Stream[F, Unit]

  /** Register a pull-based numeric gauge (Dropwizard + otel4s `ObservableGauge`) and emit unit when
    * registration succeeds.
    */
  def numericGauge(name: String, fa: F[Long], f: Endo[NumericGauge.Builder] = identity): Stream[F, Unit]

  /** Register a health check and emit unit when registration succeeds. */
  def healthCheck(name: String, f: HealthCheck.Builder => HealthCheck.Registered[F]): Stream[F, Unit]

  /** Register a percentile gauge and emit its handle. */
  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Stream[F, Percentile[F]]

  /** Register an idle-time gauge and emit its handle. */
  def idleGauge(name: String, f: Endo[IdleGauge.Builder] = identity): Stream[F, IdleGauge[F]]

  /** Register an active-time gauge and emit its handle. */
  def activeGauge(name: String, f: Endo[ActiveGauge.Builder] = identity): Stream[F, ActiveGauge[F]]

  /** Register a tag-based frequency counter and emit its handle. */
  def frequencyCounter(name: String, f: Endo[FrequencyCounter.Builder]): Stream[F, FrequencyCounter[F]]

  /** Register a two-sided balance gauge and emit its transfer handle. */
  def balanceGauge[A: {Group, Encoder}](
    source: (String, A),
    target: (String, A)): Stream[F, BalanceGauge[F, A]]
}

object MetricsHubS {

  /** Create the stream interface backed by a resource-based metrics hub. */
  def apply[F[_]](hub: MetricsHub[F])(using MonadCancel[F, Throwable]): MetricsHubS[F] =
    new MetricsHubS[F] {

      override val scope: MetricScope = hub.scope

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

      override def numericGauge(name: String, fa: F[Long], f: Endo[NumericGauge.Builder]): Stream[F, Unit] =
        Stream.resource(hub.numericGauge(name, fa, f))

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

      override def frequencyCounter(
        name: String,
        f: Endo[FrequencyCounter.Builder]): Stream[F, FrequencyCounter[F]] =
        Stream.resource(hub.frequencyCounter(name, f))

      override def balanceGauge[A: {Group, Encoder}](
        source: (String, A),
        target: (String, A)): Stream[F, BalanceGauge[F, A]] =
        Stream.resource(hub.balanceGauge(source, target))
    }
}
