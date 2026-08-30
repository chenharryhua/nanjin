package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Async, Resource}
import cats.effect.std.Dispatcher
import cats.kernel.Group
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.metrics.api.gauges.{
  ActiveGauge,
  BalanceGauge,
  FrequencyCounter,
  Gauge,
  GaugeParams,
  HealthCheck,
  IdleGauge,
  NumericGauge,
  Percentile
}
import com.github.chenharryhua.nanjin.guard.metrics.api.{Counter, Histogram, Meter, Timer}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.github.timwspence.cats.stm.STM
import org.typelevel.otel4s.metrics.MeterProvider

import java.time.ZoneId

/** Resource-based factory for metrics registered under one `MetricLabel`.
  *
  * Obtain a hub from `Agent.metricsHub(label)`, acquire an instrument with its `Resource`, and update it
  * inside the resource scope:
  *
  * {{ agent.metricsHub("requests").counter("total").use(_.inc(1)) }}
  *
  * Releasing the resource unregisters the metric. Use `MetricsHubS` when a stream-based registration API is
  * more convenient.
  *
  * ===Dual backend===
  * The push-based instruments record to Dropwizard and, when an OpenTelemetry
  * `org.typelevel.otel4s.metrics.MeterProvider` has been configured via `ServiceConfig.withMeterProvider`,
  * also to otel4s. The default provider is a no-op, so the OpenTelemetry arm is free until it is enabled. The
  * dual-write lives inside each instrument's implementation. The instrument-to-otel4s mapping is:
  *   - counter → `UpDownCounter`
  *   - meter → monotonic `Counter`
  *   - histogram → `Histogram`
  *   - timer → `Histogram` of durations in seconds
  *   - numericGauge → `ObservableGauge`
  *
  * This mapping is intentional but '''not''' a faithful one-to-one correspondence; the two backends diverge by
  * design in several places:
  *   - '''Counter → UpDownCounter''': the Dropwizard counter is reset on the reporting-window policy, while
  *     the otel `UpDownCounter` is never reset (OpenTelemetry instruments are cumulative and the backend owns
  *     windowing). The Dropwizard value is therefore windowed and the otel value cumulative.
  *   - '''numericGauge''': the Dropwizard gauge is read on scrape and holds its last value, whereas the otel
  *     `ObservableGauge` only reports while its callback registration is alive and records nothing after the
  *     registering `Resource` is released.
  *   - '''Errors''': a Dropwizard gauge can render a JSON stacktrace when evaluation fails; an otel point
  *     cannot, so it simply records nothing for that observation.
  *   - '''Loss of dimensions''': per-metric distinctions that Dropwizard keeps in the identifier (for example
  *     risk vs. normal counters) are not stamped as otel point attributes, so such siblings collapse into a
  *     single otel series unless given distinct metric names.
  *
  * The JSON `gauge` (and gauges derived from it, e.g. healthCheck/percentile/idle/active/frequency) is
  * Dropwizard-only: an arbitrary encoded value cannot satisfy otel4s's numeric `MeasurementValue`. Use
  * `numericGauge` when the value is a `Long` and should also reach OpenTelemetry.
  */
sealed trait MetricsHub[F[_]] {

  /** Metric label shared by instruments created from this hub. */
  def metricLabel: MetricLabel

  /** Register a counter; the returned counter is safe to update in `F`. */
  def counter(name: String, f: Endo[Counter.Builder] = identity): Resource[F, Counter[F]]

  /** Register a rate meter with effectful updates. */
  def meter(name: String, f: Endo[Meter.Builder] = identity): Resource[F, Meter[F]]

  /** Register a histogram with effectful updates. */
  def histogram(name: String, f: Endo[Histogram.Builder] = identity): Resource[F, Histogram[F]]

  /** Register a timer with effectful updates. */
  def timer(name: String, f: Endo[Timer.Builder] = identity): Resource[F, Timer[F]]

  /** Register a custom effectful gauge. The resource unregisters it on release. */
  def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Resource[F, Unit]

  /** Register a pull-based numeric gauge that records to Dropwizard and, when a `MeterProvider` is configured,
    * an otel4s `ObservableGauge`. The value type is constrained to a numeric type (`Long`/`Double`) because
    * OpenTelemetry gauges only accept those.
    */
  def numericGauge(
    name: String,
    fa: F[Long],
    f: Endo[NumericGauge.Builder] = identity): Resource[F, Unit]

  /** Register a boolean health check with timeout and optional refresh policy. */
  def healthCheck(name: String, f: HealthCheck.Builder => HealthCheck.Registered[F]): Resource[F, Unit]

  /** Register a numerator/denominator percentile gauge. */
  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Resource[F, Percentile[F]]

  /** Register a gauge reporting elapsed time since the last `wakeUp`. */
  def idleGauge(name: String, f: Endo[IdleGauge.Builder] = identity): Resource[F, IdleGauge[F]]

  /** Register a gauge reporting elapsed time since acquisition until `deactivate`. */
  def activeGauge(name: String, f: Endo[ActiveGauge.Builder] = identity): Resource[F, ActiveGauge[F]]

  /** Register a tag-based frequency counter reported as a JSON map gauge.
    *
    * Each call to `inc(tag)` increments that tag's count. The map resets on each policy tick.
    */
  def frequencyCounter(
    name: String,
    f: Endo[FrequencyCounter.Builder] = identity): Resource[F, FrequencyCounter[F]]

  /** Register a transactional gauge backed by a supplied STM runtime and variable. */
  def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]]

  /** Register a two-sided balance gauge and return operations to move values between sides. */
  def balanceGauge[A: {Group, Encoder}](
    source: (String, A),
    target: (String, A)): Resource[F, BalanceGauge[F, A]]
}

object MetricsHub {
  def apply[F[_]: Async](
    metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId,
    meterProvider: MeterProvider[F]): MetricsHub[F] =
    new Impl[F](metricLabel, metricRegistry, dispatcher, zoneId, meterProvider)

  private class Impl[F[_]: Async](
    val metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId,
    meterProvider: MeterProvider[F])
      extends MetricsHub[F] {

    override def counter(name: String, f: Endo[Counter.Builder]): Resource[F, Counter[F]] =
      Counter[F](metricRegistry, metricLabel, name, zoneId, meterProvider, f)

    override def meter(name: String, f: Endo[Meter.Builder]): Resource[F, Meter[F]] =
      Meter[F](metricRegistry, metricLabel, name, meterProvider, f)

    override def histogram(name: String, f: Endo[Histogram.Builder]): Resource[F, Histogram[F]] =
      Histogram[F](metricRegistry, metricLabel, name, meterProvider, f)

    override def timer(name: String, f: Endo[Timer.Builder]): Resource[F, Timer[F]] =
      Timer[F](metricRegistry, metricLabel, name, meterProvider, f)

    // gauges

    private val gaugeParams = GaugeParams[F](dispatcher, metricRegistry, metricLabel, zoneId)

    override def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Resource[F, Unit] =
      Gauge[F](gaugeParams, name, f)

    override def numericGauge(
      name: String,
      fa: F[Long],
      f: Endo[NumericGauge.Builder]): Resource[F, Unit] =
      NumericGauge[F](metricRegistry, metricLabel, name, dispatcher, meterProvider, fa, f)

    override def healthCheck(
      name: String,
      f: HealthCheck.Builder => HealthCheck.Registered[F]): Resource[F, Unit] =
      HealthCheck[F](gaugeParams, name, f)

    override def percentile(name: String, f: Endo[Percentile.Builder]): Resource[F, Percentile[F]] =
      Percentile(gaugeParams, name, f)

    // derived

    override def idleGauge(name: String, f: Endo[IdleGauge.Builder]): Resource[F, IdleGauge[F]] =
      IdleGauge(gaugeParams, name, f)

    override def activeGauge(name: String, f: Endo[ActiveGauge.Builder]): Resource[F, ActiveGauge[F]] =
      ActiveGauge(gaugeParams, name, f)

    override def frequencyCounter(
      name: String,
      f: Endo[FrequencyCounter.Builder]): Resource[F, FrequencyCounter[F]] =
      FrequencyCounter(gaugeParams, name, f)

    override def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]] =
      for {
        ta <- Resource.eval(stm.commit(stm.TVar.of(initial)))
        _ <- gauge(name, _.register(stm.commit(ta.get)))
      } yield ta

    override def balanceGauge[A: {Group, Encoder}](
      source: (String, A),
      target: (String, A)): Resource[F, BalanceGauge[F, A]] = {
      val (sourceName, sourceValue) = source
      val (targetName, targetValue) = target
      for {
        stm <- Resource.eval(STM.runtime[F])
        src <- Resource.eval(stm.commit(stm.TVar.of(sourceValue)))
        tgt <- Resource.eval(stm.commit(stm.TVar.of(targetValue)))
        _ <- gauge(
          s"Balance($sourceName<->$targetName)",
          _.register {
            val get: stm.Txn[Json] = for {
              a <- src.get
              b <- tgt.get
            } yield List(a, b).asJson
            stm.commit(get)
          })
      } yield new BalanceGauge[F, A] {
        private def transfer(from: stm.TVar[A], to: stm.TVar[A], num: A): stm.Txn[Unit] =
          for {
            _ <- from.modify(x => Group[A].combine(x, Group[A].inverse(num)))
            _ <- to.modify(y => Group[A].combine(y, num))
          } yield ()

        override def forward(num: A): F[Unit] =
          stm.commit(transfer(src, tgt, num))

        override def backward(num: A): F[Unit] =
          stm.commit(transfer(tgt, src, num))
      }
    }
  }
}
