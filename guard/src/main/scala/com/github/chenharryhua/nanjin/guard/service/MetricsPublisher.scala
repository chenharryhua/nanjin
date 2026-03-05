package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Async
import cats.effect.std.{AtomicCell, Console}
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.flatMap.toFlatMapOps
import cats.syntax.functor.toFunctorOps
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Kind.{Report, Reset}
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.{Index, Kind}
import com.github.chenharryhua.nanjin.guard.event.{Event, ScrapeMode, Snapshot, Took}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue

import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}

final private class MetricsPublisher[F[_]] private (
  val metricRegistry: MetricRegistry,
  val serviceParams: ServiceParams,
  metricsHistory: AtomicCell[F, CircularFifoQueue[MetricsSnapshot]],
  channel: Channel[F, Event],
  logSink: LogSink[F]
)(implicit F: Async[F])
    extends AdhocMetrics[F] {

  private val report_kind: Kind = Report(serviceParams.servicePolicies.metricsReport)
  private val reset_kind: Kind = Reset(serviceParams.servicePolicies.metricsReset)

  private def publish(kind: Kind, index: Index, mode: ScrapeMode): F[MetricsSnapshot] =
    for {
      ms <- Snapshot.timed[F](metricRegistry, mode).map { case (took, snapshot) =>
        MetricsSnapshot(
          index = index,
          serviceParams = serviceParams,
          snapshot = snapshot,
          kind = kind,
          took = Took(took))
      }
      _ <- channel.send(ms)
      _ <- logSink.write(ms)
      _ <- metricsHistory.modify(queue => (queue, queue.add(ms)))
    } yield ms

  private def tickingBy(kind: Kind): Stream[F, Tick] =
    tickStream.tickScheduled[F](serviceParams.zoneId, _.fresh(kind.policy))

  /*
   * Report
   */

  def report_adhoc: F[MetricsSnapshot] =
    serviceParams.zonedNow.flatMap { ts =>
      publish(report_kind, Adhoc(ts), ScrapeMode.Full)
    }

  def report_periodically: Stream[F, Nothing] =
    tickingBy(report_kind).evalMap { tick =>
      publish(report_kind, Periodic(tick), ScrapeMode.Full)
    }.drain

  /*
   * Reset
   */

  private val reset_counters: F[Unit] =
    F.blocking(metricRegistry.getCounters().values().asScala.foreach(c => c.dec(c.getCount)))

  def reset_adhoc: F[MetricsSnapshot] =
    serviceParams.zonedNow.flatMap { ts =>
      publish(reset_kind, Adhoc(ts), ScrapeMode.Full).flatTap(_ => reset_counters)
    }

  def reset_periodically: Stream[F, Nothing] =
    tickingBy(reset_kind).evalMap { tick =>
      publish(reset_kind, Periodic(tick), ScrapeMode.Full).flatTap(_ => reset_counters)
    }.drain

  /*
   * History
   */

  def get_snapshot_history: F[List[MetricsSnapshot]] =
    metricsHistory.get.map(_.iterator().asScala.toList)

  /*
   * API
   */
  override def reset: F[Unit] = reset_adhoc.void
  override def report: F[Unit] = report_adhoc.void
  override def cheapSnapshot(tick: Tick): F[MetricsSnapshot] =
    Snapshot.timed[F](metricRegistry, ScrapeMode.Cheap).map { case (took, snapshot) =>
      MetricsSnapshot(
        index = Periodic(tick),
        serviceParams = serviceParams,
        snapshot = snapshot,
        kind = report_kind,
        took = Took(took))
    }

}

private object MetricsPublisher {
  def apply[F[_]: Async: Console](
    serviceParams: ServiceParams,
    metricRegistry: MetricRegistry,
    channel: Channel[F, Event]): Stream[F, MetricsPublisher[F]] = {
    val cell: F[AtomicCell[F, CircularFifoQueue[MetricsSnapshot]]] =
      AtomicCell[F].of(new CircularFifoQueue[MetricsSnapshot](serviceParams.historyCapacity.metric))

    Stream.eval((cell, log_sink(serviceParams)).mapN { case (metricsHistory, logSink) =>
      new MetricsPublisher[F](
        metricRegistry = metricRegistry,
        serviceParams = serviceParams,
        metricsHistory = metricsHistory,
        channel = channel,
        logSink = logSink)
    })
  }
}
