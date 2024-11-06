package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref, Resource}
import cats.syntax.all.*
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.DurationFormatter
import com.github.chenharryhua.nanjin.guard.config.MetricLabel
import com.github.chenharryhua.nanjin.guard.event.NJUnits
import com.github.chenharryhua.nanjin.guard.translator.decimal_fmt

import scala.concurrent.duration.DurationInt

trait KleisliLike[F[_], A] {
  def run(a: A): F[Unit]

  final def kleisli[B](f: B => A): Kleisli[F, B, Unit] =
    Kleisli(run).local(f)

  final val kleisli: Kleisli[F, A, Unit] = Kleisli(run)
}

sealed trait Metrics[F[_]] {
  def metricLabel: MetricLabel

  def counter(name: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]]
  final def counter(name: String): Resource[F, NJCounter[F]] = counter(name, identity)

  def meter(name: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]]
  final def meter(name: String): Resource[F, NJMeter[F]] = meter(name, identity)

  def histogram(name: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]]
  final def histogram(name: String): Resource[F, NJHistogram[F]] =
    histogram(name, identity)

  def timer(name: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]]
  final def timer(name: String): Resource[F, NJTimer[F]] = timer(name, identity)

  // gauges
  def gauge(name: String, f: Endo[NJGauge.Builder]): NJGauge[F]
  final def gauge(name: String): NJGauge[F] = gauge(name, identity)

  def ratio(name: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]]
  final def ratio(name: String): Resource[F, NJRatio[F]] = ratio(name, identity)

  def healthCheck(name: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F]
  final def healthCheck(name: String): NJHealthCheck[F] = healthCheck(name, identity)

  def idleGauge(name: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Unit, Unit]]
  final def idleGauge(name: String): Resource[F, Kleisli[F, Unit, Unit]] =
    idleGauge(name, identity[NJGauge.Builder])

  def activeGauge(name: String, f: Endo[NJGauge.Builder]): Resource[F, Unit]
  final def activeGauge(name: String): Resource[F, Unit] = activeGauge(name, identity)

  def permanentCounter(name: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Long, Unit]]
  final def permanentCounter(name: String): Resource[F, Kleisli[F, Long, Unit]] =
    permanentCounter(name, identity)
}

object Metrics {
  private[guard] class Impl[F[_]](
    val metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    isEnabled: Boolean)(implicit F: Async[F])
      extends Metrics[F] {

    override def counter(name: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]] = {
      val init = new NJCounter.Builder(isEnabled, false)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def meter(name: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]] = {
      val init = new NJMeter.Builder(isEnabled, NJUnits.COUNT)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def histogram(name: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]] = {
      val init = new NJHistogram.Builder(isEnabled, NJUnits.COUNT, None)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def timer(name: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]] = {
      val init = new NJTimer.Builder(isEnabled, None)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def healthCheck(name: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F] = {
      val init = new NJHealthCheck.Builder(isEnabled, timeout = 5.seconds)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def ratio(name: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]] = {
      val init = new NJRatio.Builder(isEnabled, NJRatio.translator)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def gauge(name: String, f: Endo[NJGauge.Builder]): NJGauge[F] = {
      val init = new NJGauge.Builder(isEnabled, timeout = 5.seconds)
      f(init).build[F](metricLabel, name, metricRegistry)
    }

    override def idleGauge(name: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Unit, Unit]] =
      for {
        lastUpdate <- Resource.eval(F.monotonic.flatMap(F.ref))
        _ <- gauge(name, f).register(
          for {
            pre <- lastUpdate.get
            now <- F.monotonic
          } yield DurationFormatter.defaultFormatter.format(now - pre)
        )
      } yield Kleisli[F, Unit, Unit](_ => F.monotonic.flatMap(lastUpdate.set))

    override def activeGauge(name: String, f: Endo[NJGauge.Builder]): Resource[F, Unit] =
      for {
        kickoff <- Resource.eval(F.monotonic)
        _ <- gauge(name, f).register(F.monotonic.map(now =>
          DurationFormatter.defaultFormatter.format(now - kickoff)))
      } yield ()

    override def permanentCounter(
      name: String,
      f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Long, Unit]] =
      for {
        ref <- Resource.eval(Ref[F].of[Long](0L))
        _ <- gauge(name, f).register(ref.get.map(decimal_fmt.format))
      } yield Kleisli((num: Long) => ref.update(_ + num))
  }
}
