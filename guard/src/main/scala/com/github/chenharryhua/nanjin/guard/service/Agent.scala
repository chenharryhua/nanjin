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
import io.circe.syntax.EncoderOps

import java.time.ZoneId

sealed trait Agent[F[_]] {
  def zoneId: ZoneId

  def withMeasurement(name: String): Agent[F]

  def batch(label: String): Batch[F]

  // tick stream
  def ticks(policy: Policy): Stream[F, Tick]
  final def ticks(f: Policy.type => Policy): Stream[F, Tick] =
    ticks(f(Policy))

  // metrics adhoc report
  def adhoc: MetricsReport[F]

  def herald: Herald[F]

  def facilitate[A](label: String)(f: Metrics[F] => A): A

  def createRetry(policy: Policy): Resource[F, Retry[F]]
  final def createRetry(f: Policy.type => Policy): Resource[F, Retry[F]] =
    createRetry(f(Policy))

  // convenience
  def simpleRetry(label: String, policy: Policy)(implicit C: Console[F]): Resource[F, SimpleRetry[F]]
  final def simpleRetry(label: String, f: Policy.type => Policy)(implicit
    C: Console[F]): Resource[F, SimpleRetry[F]] =
    simpleRetry(label, f(Policy))
}

final private class GeneralAgent[F[_]: Async] private[service] (
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  measurement: Measurement)
    extends Agent[F] {

  override val zoneId: ZoneId = serviceParams.zoneId

  override def withMeasurement(name: String): Agent[F] =
    new GeneralAgent[F](serviceParams, metricRegistry, channel, Measurement(name))

  override def batch(label: String): Batch[F] = {
    val metricLabel = MetricLabel(label, measurement)
    new Batch[F](new Metrics.Impl[F](metricLabel, metricRegistry))
  }

  override def ticks(policy: Policy): Stream[F, Tick] =
    tickStream[F](TickStatus(serviceParams.zerothTick).renewPolicy(policy))

  override object adhoc extends MetricsReport[F](channel, serviceParams, metricRegistry)

  override def createRetry(policy: Policy): Resource[F, Retry[F]] =
    Resource.pure(new Retry.Impl[F](serviceParams.initialStatus.renewPolicy(policy)))

  override def facilitate[A](label: String)(f: Metrics[F] => A): A = {
    val metricLabel = MetricLabel(label, measurement)
    f(new Metrics.Impl[F](metricLabel, metricRegistry))
  }

  override val herald: Herald[F] =
    new Herald.Impl[F](serviceParams, channel)

  def simpleRetry(label: String, policy: Policy)(implicit C: Console[F]): Resource[F, SimpleRetry[F]] =
    facilitate(label) { fac =>
      val F = Async[F]
      for {
        failCounter <- fac.permanentCounter("failed")
        cancelCounter <- fac.permanentCounter("canceled")
        retryCounter <- fac.counter("retries")
        timer <- fac.timer("timer")
        retry <- createRetry(policy)
      } yield new SimpleRetry[F] {
        override def apply[A](fa: F[A]): F[A] =
          F.guaranteeCase[A](retry { (tick: Tick, ot: Option[Throwable]) =>
            ot match {
              case Some(ex) =>
                val json = Json.obj(
                  "retries" -> tick.index.asJson,
                  "snoozed" -> fmt.format(tick.snooze).asJson,
                  "measurement" -> fac.metricLabel.measurement.value.asJson,
                  "label" -> fac.metricLabel.label.asJson,
                  "policy" -> policy.show.asJson
                )
                retryCounter.inc(1) *>
                  herald.consoleWarn(ex)(json) *>
                  timer.timing(fa).map(Right(_))
              case None => timer.timing(fa).map(Right(_))
            }
          }) {
            case Outcome.Succeeded(_) => F.unit
            case Outcome.Errored(e) =>
              failCounter.inc(1) *>
                herald.error(e)(s"${fac.metricLabel.label} was failed")
            case Outcome.Canceled() =>
              cancelCounter.inc(1) *>
                herald.consoleWarn(s"${fac.metricLabel.label} was canceled")
          }
      }
    }
}
