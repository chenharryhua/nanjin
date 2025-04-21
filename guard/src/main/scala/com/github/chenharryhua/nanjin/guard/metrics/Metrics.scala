package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter

trait KleisliLike[F[_], A] {
  def run(a: A): F[Unit]

  final def kleisli[B](f: B => A): Kleisli[F, B, Unit] =
    Kleisli(run).local(f)

  final val kleisli: Kleisli[F, A, Unit] = Kleisli(run)
}

trait Metrics[F[_]] {
  def metricLabel: MetricLabel

  def counter(name: String, f: Endo[Counter.Builder]): Resource[F, Counter[F]]
  final def counter(name: String): Resource[F, Counter[F]] = counter(name, identity)

  def meter(name: String, f: Endo[Meter.Builder]): Resource[F, Meter[F]]
  final def meter(name: String): Resource[F, Meter[F]] = meter(name, identity)

  def histogram(name: String, f: Endo[Histogram.Builder]): Resource[F, Histogram[F]]
  final def histogram(name: String): Resource[F, Histogram[F]] =
    histogram(name, identity)

  def timer(name: String, f: Endo[Timer.Builder]): Resource[F, Timer[F]]
  final def timer(name: String): Resource[F, Timer[F]] = timer(name, identity)

  // gauges
  def gauge(name: String, f: Endo[Gauge.Builder]): Gauge[F]
  final def gauge(name: String): Gauge[F] = gauge(name, identity)

  def percentile(name: String, f: Endo[Percentile.Builder]): Resource[F, Percentile[F]]
  final def percentile(name: String): Resource[F, Percentile[F]] = percentile(name, identity)

  def healthCheck(name: String, f: Endo[HealthCheck.Builder]): HealthCheck[F]
  final def healthCheck(name: String): HealthCheck[F] = healthCheck(name, identity)

  def idleGauge(name: String, f: Endo[Gauge.Builder]): Resource[F, IdleGauge[F]]
  final def idleGauge(name: String): Resource[F, IdleGauge[F]] =
    idleGauge(name, identity[Gauge.Builder])

  def activeGauge(name: String, f: Endo[Gauge.Builder]): Resource[F, Unit]
  final def activeGauge(name: String): Resource[F, Unit] = activeGauge(name, identity)

  def permanentCounter(name: String, f: Endo[Gauge.Builder]): Resource[F, Counter[F]]
  final def permanentCounter(name: String): Resource[F, Counter[F]] =
    permanentCounter(name, identity)
}

object Metrics {
  private[guard] class Impl[F[_]: Async](
    val metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F])
      extends Metrics[F] {
    private[this] val F = Async[F]

    override def counter(name: String, f: Endo[Counter.Builder]): Resource[F, Counter[F]] =
      f(Counter.initial).build[F](metricLabel, name, metricRegistry)

    override def meter(name: String, f: Endo[Meter.Builder]): Resource[F, Meter[F]] =
      f(Meter.initial).build[F](metricLabel, name, metricRegistry)

    override def histogram(name: String, f: Endo[Histogram.Builder]): Resource[F, Histogram[F]] =
      f(Histogram.initial).build[F](metricLabel, name, metricRegistry)

    override def timer(name: String, f: Endo[Timer.Builder]): Resource[F, Timer[F]] =
      f(Timer.initial).build[F](metricLabel, name, metricRegistry)

    // gauges

    override def healthCheck(name: String, f: Endo[HealthCheck.Builder]): HealthCheck[F] =
      f(HealthCheck.initial).build[F](metricLabel, name, metricRegistry, dispatcher)

    override def percentile(name: String, f: Endo[Percentile.Builder]): Resource[F, Percentile[F]] =
      f(Percentile.initial).build[F](metricLabel, name, metricRegistry, dispatcher)

    override def gauge(name: String, f: Endo[Gauge.Builder]): Gauge[F] =
      f(Gauge.initial).build[F](metricLabel, name, metricRegistry, dispatcher)

    // derived

    override def idleGauge(name: String, f: Endo[Gauge.Builder]): Resource[F, IdleGauge[F]] =
      for {
        lastUpdate <- Resource.eval(F.monotonic.flatMap(F.ref))
        _ <- gauge(name, f).register(
          for {
            pre <- lastUpdate.get
            now <- F.monotonic
          } yield durationFormatter.format(now - pre)
        )
      } yield new IdleGauge[F] {
        override val wakeUp: F[Unit] = F.monotonic.flatMap(lastUpdate.set)
      }

    override def activeGauge(name: String, f: Endo[Gauge.Builder]): Resource[F, Unit] =
      for {
        kickoff <- Resource.eval(F.monotonic)
        _ <- gauge(name, f).register(F.monotonic.map(now => durationFormatter.format(now - kickoff)))
      } yield ()

    override def permanentCounter(name: String, f: Endo[Gauge.Builder]): Resource[F, Counter[F]] =
      for {
        ref <- Resource.eval(Ref[F].of[Long](0L))
        _ <- gauge(name, f).register(ref.get)
      } yield new Counter[F] {
        override def inc(num: Long): F[Unit] = ref.update(_ + num)
      }
  }
}
