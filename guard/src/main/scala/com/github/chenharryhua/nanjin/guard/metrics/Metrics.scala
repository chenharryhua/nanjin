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
  def metricName: MetricLabel

  def counter(tag: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]]
  final def counter(tag: String): Resource[F, NJCounter[F]] = counter(tag, identity)

  def meter(tag: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]]
  final def meter(tag: String): Resource[F, NJMeter[F]] = meter(tag, identity)

  def histogram(tag: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]]
  final def histogram(tag: String): Resource[F, NJHistogram[F]] =
    histogram(tag, identity)

  def timer(tag: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]]
  final def timer(tag: String): Resource[F, NJTimer[F]] = timer(tag, identity)

  // gauges
  def gauge(tag: String, f: Endo[NJGauge.Builder]): NJGauge[F]
  final def gauge(tag: String): NJGauge[F] = gauge(tag, identity)

  def ratio(tag: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]]
  final def ratio(tag: String): Resource[F, NJRatio[F]] = ratio(tag, identity)

  def healthCheck(tag: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F]
  final def healthCheck(tag: String): NJHealthCheck[F] = healthCheck(tag, identity)

  def idleGauge(tag: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Unit, Unit]]
  final def idleGauge(tag: String): Resource[F, Kleisli[F, Unit, Unit]] =
    idleGauge(tag, identity[NJGauge.Builder])

  def activeGauge(tag: String, f: Endo[NJGauge.Builder]): Resource[F, Unit]
  final def activeGauge(tag: String): Resource[F, Unit] = activeGauge(tag, identity)

  def permanentCounter(tag: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Long, Unit]]
  final def permanentCounter(tag: String): Resource[F, Kleisli[F, Long, Unit]] =
    permanentCounter(tag, identity)
}

object Metrics {
  private[guard] class Impl[F[_]](
    val metricName: MetricLabel,
    metricRegistry: MetricRegistry,
    isEnabled: Boolean)(implicit F: Async[F])
      extends Metrics[F] {

    override def counter(tag: String, f: Endo[NJCounter.Builder]): Resource[F, NJCounter[F]] = {
      val init = new NJCounter.Builder(isEnabled, false)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def meter(tag: String, f: Endo[NJMeter.Builder]): Resource[F, NJMeter[F]] = {
      val init = new NJMeter.Builder(isEnabled, NJUnits.COUNT)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def histogram(tag: String, f: Endo[NJHistogram.Builder]): Resource[F, NJHistogram[F]] = {
      val init = new NJHistogram.Builder(isEnabled, NJUnits.COUNT, None)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def timer(tag: String, f: Endo[NJTimer.Builder]): Resource[F, NJTimer[F]] = {
      val init = new NJTimer.Builder(isEnabled, None)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def healthCheck(tag: String, f: Endo[NJHealthCheck.Builder]): NJHealthCheck[F] = {
      val init = new NJHealthCheck.Builder(isEnabled, timeout = 5.seconds)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def ratio(tag: String, f: Endo[NJRatio.Builder]): Resource[F, NJRatio[F]] = {
      val init = new NJRatio.Builder(isEnabled, NJRatio.translator)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def gauge(tag: String, f: Endo[NJGauge.Builder]): NJGauge[F] = {
      val init = new NJGauge.Builder(isEnabled, timeout = 5.seconds)
      f(init).build[F](metricName, tag, metricRegistry)
    }

    override def idleGauge(tag: String, f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Unit, Unit]] =
      for {
        lastUpdate <- Resource.eval(F.monotonic.flatMap(F.ref))
        _ <- gauge(tag, f).register(
          for {
            pre <- lastUpdate.get
            now <- F.monotonic
          } yield DurationFormatter.defaultFormatter.format(now - pre)
        )
      } yield Kleisli[F, Unit, Unit](_ => F.monotonic.flatMap(lastUpdate.set))

    override def activeGauge(tag: String, f: Endo[NJGauge.Builder]): Resource[F, Unit] =
      for {
        kickoff <- Resource.eval(F.monotonic)
        _ <- gauge(tag, f).register(F.monotonic.map(now =>
          DurationFormatter.defaultFormatter.format(now - kickoff)))
      } yield ()

    override def permanentCounter(
      tag: String,
      f: Endo[NJGauge.Builder]): Resource[F, Kleisli[F, Long, Unit]] =
      for {
        ref <- Resource.eval(Ref[F].of[Long](0L))
        _ <- gauge(tag, f).register(ref.get.map(decimal_fmt.format))
      } yield Kleisli((num: Long) => ref.update(_ + num))
  }
}
