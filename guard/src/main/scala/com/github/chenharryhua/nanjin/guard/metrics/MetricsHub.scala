package com.github.chenharryhua.nanjin.guard.metrics

import cats.Endo
import cats.effect.kernel.{Async, Ref, Resource}
import cats.effect.std.Dispatcher
import cats.kernel.Group
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.guard.event.MetricLabel
import com.github.chenharryhua.nanjin.guard.metrics.gauges.{
  ActiveGauge,
  BalanceGauge,
  Gauge,
  GaugeParams,
  HealthCheck,
  IdleGauge,
  Percentile
}
import io.circe.syntax.EncoderOps
import io.circe.{Encoder, Json}
import io.github.timwspence.cats.stm.STM

import java.time.ZoneId

sealed trait MetricsHub[F[_]] {
  def metricLabel: MetricLabel

  def counter(name: String, f: Endo[Counter.Builder] = identity): Resource[F, Counter[F]]

  def meter(name: String, f: Endo[Meter.Builder] = identity): Resource[F, Meter[F]]

  def histogram(name: String, f: Endo[Histogram.Builder] = identity): Resource[F, Histogram[F]]

  def timer(name: String, f: Endo[Timer.Builder] = identity): Resource[F, Timer[F]]

  // gauges

  def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Resource[F, Unit]

  def healthCheck(name: String, f: HealthCheck.Builder => HealthCheck.Registered[F]): Resource[F, Unit]

  def percentile(name: String, f: Endo[Percentile.Builder] = identity): Resource[F, Percentile[F]]

  def idleGauge(name: String, f: Endo[IdleGauge.Builder] = identity): Resource[F, IdleGauge[F]]

  def activeGauge(name: String, f: Endo[ActiveGauge.Builder] = identity): Resource[F, ActiveGauge[F]]

  def permanentCounter(name: String): Resource[F, Counter[F]]

  def txnGauge[A: Encoder](stm: STM[F], initial: A)(name: String): Resource[F, stm.TVar[A]]
  def balanceGauge[A: {Group, Encoder}](
    source: (String, A),
    target: (String, A)): Resource[F, BalanceGauge[F, A]]
}

object MetricsHub {
  def apply[F[_]: Async](
    metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId): MetricsHub[F] =
    new Impl[F](metricLabel, metricRegistry, dispatcher, zoneId)

  private class Impl[F[_]: Async](
    val metricLabel: MetricLabel,
    metricRegistry: MetricRegistry,
    dispatcher: Dispatcher[F],
    zoneId: ZoneId)
      extends MetricsHub[F] {

    override def counter(name: String, f: Endo[Counter.Builder]): Resource[F, Counter[F]] =
      Counter[F](metricRegistry, metricLabel, name, f)

    override def meter(name: String, f: Endo[Meter.Builder]): Resource[F, Meter[F]] =
      Meter[F](metricRegistry, metricLabel, name, f)

    override def histogram(name: String, f: Endo[Histogram.Builder]): Resource[F, Histogram[F]] =
      Histogram[F](metricRegistry, metricLabel, name, f)

    override def timer(name: String, f: Endo[Timer.Builder]): Resource[F, Timer[F]] =
      Timer[F](metricRegistry, metricLabel, name, f)

    // gauges

    private val gaugeParams = GaugeParams[F](dispatcher, metricRegistry, metricLabel, zoneId)

    override def gauge(name: String, f: Gauge.Builder => Gauge.Registered[F]): Resource[F, Unit] =
      Gauge[F](gaugeParams, name, f)

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

    override def permanentCounter(name: String): Resource[F, Counter[F]] =
      for {
        ref <- Resource.eval(Ref[F].of[Long](0L))
        _ <- gauge(name, _.register(ref.get))
      } yield new Counter[F] {
        override def inc(num: Long): F[Unit] = ref.update(_ + num)
      }

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
