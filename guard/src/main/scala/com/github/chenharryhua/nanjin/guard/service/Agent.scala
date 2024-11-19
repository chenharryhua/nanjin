package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Outcome, Resource}
import cats.effect.std.Console
import cats.implicits.{catsSyntaxApplyOps, toFunctorOps, toShow}
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.*
import com.github.chenharryhua.nanjin.guard.action.*
import com.github.chenharryhua.nanjin.guard.config.*
import com.github.chenharryhua.nanjin.guard.event.*
import com.github.chenharryhua.nanjin.guard.metrics.Metrics
import com.github.chenharryhua.nanjin.guard.translator.fmt
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Json

import java.time.ZoneId
import scala.concurrent.duration.FiniteDuration

sealed trait Agent[F[_]] {
  def zoneId: ZoneId

  def withMeasurement(name: String): Agent[F]

  def batch(label: String): Batch[F]

  /** start from first tick
    */
  def ticks(policy: Policy): Stream[F, Tick]
  final def ticks(f: Policy.type => Policy): Stream[F, Tick] =
    ticks(f(Policy))

  /** start from zeroth tick immediately
    */
  def tickImmediately(policy: Policy): Stream[F, Tick]
  final def tickImmediately(f: Policy.type => Policy): Stream[F, Tick] =
    tickImmediately(f(Policy))

  // metrics adhoc report
  def adhoc: MetricsReport[F]

  def herald: Herald[F]

  def facilitate[A](label: String)(f: Metrics[F] => A): A

  def createRetry(policy: Policy): Resource[F, Retry[F]]
  final def createRetry(f: Policy.type => Policy): Resource[F, Retry[F]] =
    createRetry(f(Policy))

  // for convenience
  def measuredRetry(label: String, policy: Policy): Resource[F, SimpleRetry[F]]
  final def measuredRetry(label: String, f: Policy.type => Policy): Resource[F, SimpleRetry[F]] =
    measuredRetry(label, f(Policy))

  def heavyRetry(label: String, policy: Policy)(implicit C: Console[F]): Resource[F, SimpleRetry[F]]
  final def heavyRetry(label: String, f: Policy.type => Policy)(implicit
    C: Console[F]): Resource[F, SimpleRetry[F]] =
    heavyRetry(label, f(Policy))
}

final private class GeneralAgent[F[_]](
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement)(implicit F: Async[F])
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name))

  override def batch(label: String): Batch[F] = {
    val metricLabel = MetricLabel(label, measurement)
    new Batch[F](new Metrics.Impl[F](metricLabel, metricRegistry))
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream.fromTickStatus[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override def tickImmediately(policy: Policy): Stream[F, Tick] =
    tickStream.fromZero(policy, zoneId)

  override def createRetry(policy: Policy): Resource[F, Retry[F]] =
    Resource.pure(new Retry.Impl[F](serviceParams.initialStatus.renewPolicy(policy)))

  override def facilitate[A](label: String)(f: Metrics[F] => A): A = {
    val metricLabel = MetricLabel(label, measurement)
    f(new Metrics.Impl[F](metricLabel, metricRegistry))
  }

  override object adhoc extends MetricsReport[F](channel, serviceParams, metricRegistry)
  override object herald extends Herald.Impl[F](serviceParams, channel)

  override def measuredRetry(label: String, policy: Policy): Resource[F, SimpleRetry[F]] =
    facilitate(label) { fac =>
      for {
        failCounter <- fac.permanentCounter("failed")
        cancelCounter <- fac.permanentCounter("canceled")
        retryCounter <- fac.counter("retries")
        recentCounter <- fac.counter("recent")
        timer <- fac.timer("timer")
        retry <- createRetry(policy)
      } yield new SimpleRetry[F] {
        override def apply[A](fa: F[A]): F[A] =
          F.guaranteeCase[(FiniteDuration, A)](retry { (_: Tick, ot: Option[Throwable]) =>
            ot match {
              case Some(_) => retryCounter.inc(1) *> F.timed(fa).map(Right(_))
              case None    => F.timed(fa).map(Right(_))
            }
          }) {
            case Outcome.Succeeded(ra) => F.flatMap(ra)(a => timer.update(a._1)) *> recentCounter.inc(1)
            case Outcome.Errored(_)    => failCounter.inc(1)
            case Outcome.Canceled()    => cancelCounter.inc(1)
          }.map(_._2)
      }
    }

  override def heavyRetry(label: String, policy: Policy)(implicit
    C: Console[F]): Resource[F, SimpleRetry[F]] =
    facilitate(label) { fac =>
      for {
        failCounter <- fac.permanentCounter("failed")
        cancelCounter <- fac.permanentCounter("canceled")
        retryCounter <- fac.counter("retries")
        recentCounter <- fac.counter("recent")
        timer <- fac.timer("timer")
        retry <- createRetry(policy)
      } yield new SimpleRetry[F] {
        override def apply[A](fa: F[A]): F[A] =
          F.guaranteeCase[(FiniteDuration, A)](retry { (tick: Tick, ot: Option[Throwable]) =>
            ot match {
              case Some(ex) =>
                val json = Json.obj(
                  "description" -> Json.fromString(s"retry $label"),
                  "nth_retry" -> Json.fromLong(tick.index),
                  "snoozed" -> Json.fromString(fmt.format(tick.snooze)),
                  "measurement" -> Json.fromString(fac.metricLabel.measurement.value),
                  "retry_policy" -> Json.fromString(policy.show)
                )
                herald.consoleWarn(ex)(json) *>
                  retryCounter.inc(1) *>
                  F.timed(fa).map(Right(_))
              case None => F.timed(fa).map(Right(_))
            }
          }) {
            case Outcome.Succeeded(ra) =>
              F.flatMap(ra)(a => timer.update(a._1)) *>
                recentCounter.inc(1)
            case Outcome.Errored(e) =>
              val json = Json.obj(
                "description" -> Json.fromString(s"$label was failed"),
                "measurement" -> Json.fromString(fac.metricLabel.measurement.value),
                "retry_policy" -> Json.fromString(policy.show)
              )
              herald.error(e)(json) *> failCounter.inc(1)
            case Outcome.Canceled() =>
              herald.error(s"$label was canceled") *> cancelCounter.inc(1)
          }.map(_._2)
      }
    }
}
