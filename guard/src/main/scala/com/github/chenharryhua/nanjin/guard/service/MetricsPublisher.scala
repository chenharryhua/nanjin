package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, Console}
import cats.effect.syntax.clock.clockOps
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Kind.{Report, Reset}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.{Index, Kind}
import com.github.chenharryhua.nanjin.guard.event.{Event, Took}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue

import scala.jdk.CollectionConverters.IteratorHasAsScala

final private class MetricsPublisher[F[_]] private (
  val serviceParams: ServiceParams,
  val scrapeMetrics: ScrapeMetrics,
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricsSnapshot]],
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
      _ <- metricsHistory.modify(queue => (queue, queue.add(ms)))
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
      build_metrics_snapshot(reset_kind, Adhoc(ts)).flatTap(_ => reset_counters)
    }

  def reset_periodically: Stream[F, Nothing] =
    tickingBy(reset_kind).evalMap { tick =>
      publish(reset_kind, Periodic(tick)).flatTap(_ => reset_counters)
    }.drain

  /*
   * History
   */

  def get_snapshot_history: F[List[MetricsSnapshot]] =
    metricsHistory.get.map(_.iterator().asScala.toList)

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

private object MetricsPublisher {
  def apply[F[_]: {Async, Console}](
    serviceParams: ServiceParams,
    scrapeMetrics: ScrapeMetrics,
    channel: Channel[F, Event]): Stream[F, MetricsPublisher[F]] = {
    val cell: F[AtomicCell[F, CircularFifoQueue[MetricsSnapshot]]] =
      AtomicCell[F].of(new CircularFifoQueue[MetricsSnapshot](serviceParams.historyCapacity.metric))

    Stream.eval((cell, log_sink(serviceParams)).mapN { case (metricsHistory, logSink) =>
      new MetricsPublisher[F](
        serviceParams = serviceParams,
        scrapeMetrics = scrapeMetrics,
        metricsHistory = metricsHistory,
        channel = channel,
        logSink = logSink)
    })
  }
}
