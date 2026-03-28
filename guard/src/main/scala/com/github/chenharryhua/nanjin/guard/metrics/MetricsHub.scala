package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.kernel.Group
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.option.{none, given}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.DurationFormatter.defaultFormatter
import com.github.chenharryhua.nanjin.guard.event.{MetricLabel, Squants}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.github.timwspence.cats.stm.STM
import squants.Each

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

trait MetricsHub[F[_]] {
  def metricLabel: MetricLabel

  def counter(name: String, f: Endo[Counter.Builder] = identity): Resource[F, Counter[F]]

  def meter(name: String, f: Endo[Meter.Builder] = identity): Resource[F, Meter[F]]

  def histogram(name: String, f: Endo[Histogram.Builder] = identity): Resource[F, Histogram[F]]

  def timer(name: String, f: Endo[Timer.Builder] = identity): Resource[F, Timer[F]]

  // gauges
  def gauge(name: String, f: Endo[Gauge.Builder] = identity): Gauge[F]

  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Resource[F, Percentile[F]]

  def healthCheck(name: String, f: Endo[HealthCheck.Builder] = identity): HealthCheck[F]

  def idleGauge(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, IdleGauge[F]]

  def activeGauge(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, ActiveGauge[F]]

  def permanentCounter(name: String, f: Endo[Gauge.Builder] = identity): Resource[F, Counter[F]]

  def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]]
  def balanceGauge[A: {Group, Encoder}](
    source: (String, A),
    target: (String, A)): Resource[F, BalanceGauge[F, A]]
}

object MetricsHub {
  private[guard] class Impl[F[_]: Async](
    val metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId)
      extends MetricsHub[F] {
    private val F = Async[F]

    override def counter(name: String, f: Endo[Counter.Builder]): Resource[F, Counter[F]] = {
      val initial: Counter.Builder = new Counter.Builder(isEnabled = true, isRisk = false)

      f(initial).build[F](metricLabel, name, metricRegistry)
    }

    override def meter(name: String, f: Endo[Meter.Builder]): Resource[F, Meter[F]] = {
      val initial = new Meter.Builder(isEnabled = true, squants = Squants(Each))
      f(initial).build[F](metricLabel, name, metricRegistry)
    }

    override def histogram(name: String, f: Endo[Histogram.Builder]): Resource[F, Histogram[F]] = {
      val initial: Histogram.Builder =
        new Histogram.Builder(isEnabled = true, squants = Squants(Each), reservoir = None)

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
          } yield defaultFormatter.format(now - pre)
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
            .ifM(F.monotonic.map(now => defaultFormatter.format(now - kickoff).some), F.pure(none[String])))
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

    override def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]] =
      for {
        ta <- Resource.eval(stm.commit(stm.TVar.of(initial)))
        _ <- gauge(name, identity).register(stm.commit(ta.get))
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
        _ <- gauge(s"Balance($sourceName<->$targetName)", identity).register {
          val get: stm.Txn[Json] = for {
            a <- src.get
            b <- tgt.get
          } yield List(a, b).asJson
          stm.commit(get)
        }
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
