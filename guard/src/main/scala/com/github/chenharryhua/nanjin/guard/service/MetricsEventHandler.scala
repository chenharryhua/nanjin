package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Async
import cats.effect.syntax.clock.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.codahale.metrics.MetricRegistry
import com.github.chenharryhua.nanjin.common.chrono.{tickStream, Tick}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index
import com.github.chenharryhua.nanjin.guard.event.MetricsEvent.Index.{Adhoc, Periodic}
import com.github.chenharryhua.nanjin.guard.event.{Event, Took}
import fs2.Stream
import fs2.concurrent.Channel

final private class MetricsEventHandler[F[_]] private (
  val serviceParams: ServiceParams,
  val scrapeMetrics: ScrapeMetrics,
  history: History[F, MetricsSnapshot],
  channel: Channel[F, Event],
  logSink: LogSink[F]
)(using F: Async[F])
    extends AdhocMetrics[F] {

  private def build_metrics_snapshot(index: Index): F[MetricsSnapshot] =
    F.blocking(scrapeMetrics.snapshot(ScrapeMode.Full)).timed.map { case (took, snapshot) =>
      MetricsSnapshot(index = index, serviceParams = serviceParams, snapshot = snapshot, took = Took(took))
    }

  private def publish(index: Index): F[MetricsSnapshot] =
    for {
      ms <- build_metrics_snapshot(index)
      _ <- channel.send(ms)
      _ <- logSink.write(ms)
      _ <- history.add(ms)
    } yield ms

  private val ticking: Stream[F, Tick] =
    tickStream.tickScheduled[F](serviceParams.zoneId, _.fresh(serviceParams.policies.report))

  /*
   * Report
   */
  def httpReport: F[MetricsSnapshot] =
    serviceParams.zonedNow.flatMap { ts =>
      build_metrics_snapshot(Adhoc(ts))
    }

  def reportPeriodically: Stream[F, Nothing] =
    ticking.evalMap { tick =>
      publish(Periodic(tick))
    }.drain

  /*
   * History
   */

  def snapshotHistory: F[Vector[MetricsSnapshot]] = history.value

  /*
   * API
   */

  override def report: F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- publish(Adhoc(ts))
    } yield ()

  override def snapshot(tick: Tick, f: ScrapeMode.type => ScrapeMode): F[MetricsSnapshot] =
    F.blocking(scrapeMetrics.snapshot(f(ScrapeMode))).timed.map { case (took, snapshot) =>
      MetricsSnapshot(
        index = Periodic(tick),
        serviceParams = serviceParams,
        snapshot = snapshot,
        took = Took(took))
    }
}

private object MetricsEventHandler {
  def apply[F[_]: Async](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F]
  ): Stream[F, MetricsEventHandler[F]] = {
    val history: F[History[F, MetricsSnapshot]] =
      History[F, MetricsSnapshot](serviceParams.history.map(_.metrics))

    Stream.eval(history).map { metricsHistory =>
      new MetricsEventHandler[F](
        serviceParams = serviceParams,
        scrapeMetrics = new ScrapeMetrics(new MetricRegistry()),
        history = metricsHistory,
        channel = channel,
        logSink = logSink)
    }
  }
}
