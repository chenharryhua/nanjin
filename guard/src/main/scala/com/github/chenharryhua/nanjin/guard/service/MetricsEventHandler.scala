package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Async
import cats.effect.std.Console
import cats.effect.syntax.clock.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Kind.{Report, Reset}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.{Index, Kind}
import com.github.chenharryhua.nanjin.guard.event.{Event, Took}
import com.github.chenharryhua.nanjin.guard.service.logging.LogSink
import fs2.Stream
import fs2.concurrent.Channel

final private class MetricsEventHandler[F[_]] private (
  val serviceParams: ServiceParams,
  val scrapeMetrics: ScrapeMetrics,
  metricsHistory: History[F, MetricsSnapshot],
  channel: Channel[F, Event],
  logSink: LogSink[F]
)(using F: Async[F])
    extends AdhocMetrics[F] {

  private val report_kind: Kind = Report(serviceParams.servicePolicies.metricsReport)
  private val reset_kind: Kind = Reset(serviceParams.servicePolicies.metricsReset)

  private def build_metrics_snapshot(kind: Kind, index: Index): F[MetricsSnapshot] =
    F.blocking(scrapeMetrics.snapshot(ScrapeMode.Full)).timed.map { case (took, snapshot) =>
      MetricsSnapshot(
        index = index,
        serviceParams = serviceParams,
        snapshot = snapshot,
        kind = kind,
        took = Took(took))
    }

  private def publish(kind: Kind, index: Index): F[MetricsSnapshot] =
    for {
      ms <- build_metrics_snapshot(kind, index)
      _ <- channel.send(ms)
      _ <- logSink.write(ms)
      _ <- metricsHistory.add(ms)
    } yield ms

  private def tickingBy(kind: Kind): Stream[F, Tick] =
    tickStream.tickScheduled[F](serviceParams.zoneId, _.fresh(kind.policy))

  /*
   * Report
   */
  def http_report: F[MetricsSnapshot] =
    serviceParams.zonedNow.flatMap { ts =>
      build_metrics_snapshot(report_kind, Adhoc(ts))
    }

  def report_periodically: Stream[F, Nothing] =
    tickingBy(report_kind).evalMap { tick =>
      publish(report_kind, Periodic(tick))
    }.drain

  /*
   * Reset
   */

  private val reset_counters: F[Unit] =
    F.blocking(scrapeMetrics.resetCounter())

  def http_reset: F[MetricsSnapshot] =
    serviceParams.zonedNow.flatMap { ts =>
      build_metrics_snapshot(reset_kind, Adhoc(ts)) <* reset_counters
    }

  def reset_periodically: Stream[F, Nothing] =
    tickingBy(reset_kind).evalMap { tick =>
      publish(reset_kind, Periodic(tick)) >> reset_counters
    }.drain

  /*
   * History
   */

  def get_snapshot_history: F[Vector[MetricsSnapshot]] =
    metricsHistory.value

  /*
   * API
   */
  override def reset: F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- publish(reset_kind, Adhoc(ts))
      _ <- reset_counters
    } yield ()

  override def report: F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- publish(reset_kind, Adhoc(ts))
    } yield ()

  override def cheapSnapshot(tick: Tick): F[MetricsSnapshot] =
    F.blocking(scrapeMetrics.snapshot(ScrapeMode.Cheap)).timed.map { case (took, snapshot) =>
      MetricsSnapshot(
        index = Periodic(tick),
        serviceParams = serviceParams,
        snapshot = snapshot,
        kind = report_kind,
        took = Took(took))
    }
}

private object MetricsEventHandler {
  def apply[F[_]: {Async, Console}](
    serviceParams: ServiceParams,
    channel: Channel[F, Event]
  ): Stream[F, MetricsEventHandler[F]] = {
    val history: F[History[F, MetricsSnapshot]] =
      History[F, MetricsSnapshot](serviceParams.historyCapacity.metric)

    Stream.eval((history, log_sink(serviceParams)).mapN { case (metricsHistory, logSink) =>
      new MetricsEventHandler[F](
        serviceParams = serviceParams,
        scrapeMetrics = new ScrapeMetrics(new MetricRegistry()),
        metricsHistory = metricsHistory,
        channel = channel,
        logSink = logSink)
    })
  }
}
