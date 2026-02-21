package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.data.Kleisli
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.syntax.functor.toFunctorOps
import cats.syntax.flatMap.{catsSyntaxIfM, toFlatMapOps}
import cats.syntax.option.{catsSyntaxOptionId, none}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import com.github.chenharryhua.nanjin.guard.translator.durationFormatter
import io.circe.Encoder
import io.github.timwspence.cats.stm.STM
import squants.{Quantity, UnitOfMeasure}

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

trait KleisliLike[F[_], A] {
  def run(a: A): F[Unit]

  final def local[B](f: B => A): Kleisli[F, B, Unit] =
    Kleisli(run).local(f)

  final val kleisli: Kleisli[F, A, Unit] = Kleisli(run)
}

trait Metrics[F[_]] {
  def metricLabel: MetricLabel

  def counter(name: String, f: Endo[Counter.Builder] = identity): Resource[F, Counter[F]]

  def meter[A <: Quantity[A]](unitOfMeasure: UnitOfMeasure[A])(
    name: String,
    f: Endo[Meter.Builder[A]] = identity[Meter.Builder[A]](_)): Resource[F, Meter[F, A]]

  def histogram[A <: Quantity[A]](unitOfMeasure: UnitOfMeasure[A])(
    name: String,
    f: Endo[Histogram.Builder[A]] = identity[Histogram.Builder[A]](_)): Resource[F, Histogram[F, A]]

  def timer(name: String, f: Endo[Timer.Builder] = identity): Resource[F, Timer[F]]

  // gauges
  def gauge(name: String, f: Endo[Gauge.Builder] = identity): Gauge[F]

  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Resource[F, Percentile[F]]

  def healthCheck(name: String, f: Endo[HealthCheck.Builder] = identity): HealthCheck[F]

  def idleGauge(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, IdleGauge[F]]

  def activeGauge(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, ActiveGauge[F]]

  def permanentCounter(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, Counter[F]]
  def deltaCounter(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, Counter[F]]

  def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]]
}

object Metrics {
  private[guard] class Impl[F[_]: Async](
    val metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId)
      extends Metrics[F] {
    private[this] val F = Async[F]

    override def counter(name: String, f: Endo[Counter.Builder]): Resource[F, Counter[F]] = {
      val initial: Counter.Builder = new Counter.Builder(isEnabled = true, isRisk = false)

      f(initial).build[F](metricLabel, name, metricRegistry)
    }

    override def meter[A <: Quantity[A]](
      unitOfMeasure: UnitOfMeasure[A])(name: String, f: Endo[Meter.Builder[A]]): Resource[F, Meter[F, A]] = {
      val initial = new Meter.Builder(isEnabled = true, unitOfMeasure = unitOfMeasure)
      f(initial).build[F](metricLabel, name, metricRegistry)
    }

    override def histogram[A <: Quantity[A]](unitOfMeasure: UnitOfMeasure[A])(
      name: String,
      f: Endo[Histogram.Builder[A]]): Resource[F, Histogram[F, A]] = {
      val initial: Histogram.Builder[A] =
        new Histogram.Builder(isEnabled = true, unitOfMeasure = unitOfMeasure, reservoir = None)

      f(initial).build[F](metricLabel, name, metricRegistry)
    }

    override def timer(name: String, f: Endo[Timer.Builder]): Resource[F, Timer[F]] = {
      val initial: Timer.Builder = new Timer.Builder(isEnabled = true, reservoir = None)
      f(initial).build[F](metricLabel, name, metricRegistry)
    }

    // gauges

    override def healthCheck(name: String, f: Endo[HealthCheck.Builder]): HealthCheck[F] = {
      val initial: HealthCheck.Builder = new HealthCheck.Builder(isEnabled = true, timeout = 5.seconds)
      f(initial).build[F](metricLabel, name, metricRegistry, dispatcher, zoneId)
    }

    override def percentile(name: String, f: Endo[Percentile.Builder]): Resource[F, Percentile[F]] = {
      val initial: Percentile.Builder =
        new Percentile.Builder(isEnabled = true, translator = Percentile.translator)
      f(initial).build[F](metricLabel, name, metricRegistry, dispatcher)
    }

    override def gauge(name: String, f: Endo[Gauge.Builder]): Gauge[F] = {
      val initial: Gauge.Builder = new Gauge.Builder(isEnabled = true, timeout = 5.seconds)
      f(initial).build[F](metricLabel, name, metricRegistry, dispatcher, zoneId)
    }

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

    override def activeGauge(name: String, f: Endo[Gauge.Builder]): Resource[F, ActiveGauge[F]] =
      for {
        active <- Resource.eval(F.ref(true))
        kickoff <- Resource.eval(F.monotonic)
        _ <- gauge(name, f).register(
          active.get
            .ifM(F.monotonic.map(now => durationFormatter.format(now - kickoff).some), F.pure(none[String])))
      } yield new ActiveGauge[F] {
        override val deactivate: F[Unit] = active.set(false)
      }

    override def permanentCounter(name: String, f: Endo[Gauge.Builder]): Resource[F, Counter[F]] =
      for {
        ref <- Resource.eval(Ref[F].of[Long](0L))
        _ <- gauge(name, f).register(ref.get)
      } yield new Counter[F] {
        override def inc(num: Long): F[Unit] = ref.update(_ + num)
      }

    override def deltaCounter(name: String, f: Endo[Gauge.Builder]): Resource[F, Counter[F]] =
      for {
        ref <- Resource.eval(Ref[F].of[Long](0L))
        _ <- gauge(name, f).register(ref.getAndSet(0L))
      } yield new Counter[F] {
        override def inc(num: Long): F[Unit] = ref.update(_ + num)
      }

    override def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]] =
      for {
        ta <- Resource.eval(stm.commit(stm.TVar.of(initial)))
        _ <- gauge(name, identity).register(stm.commit(ta.get))
      } yield ta
  }
}
