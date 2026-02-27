package com.github.chenharryhua.nanjin.guard.service

import cats.Functor
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot

/** adhoc metrics report and reset
  */
sealed trait AdhocMetrics[F[_]] {

  /** reset all counters to zero
    */
  def reset: F[Unit]

  /** report current metrics
    */
  def report: F[Unit]

  def cheapSnapshot(tick: Tick): F[MetricsSnapshot]
}

final private class AdhocMetricsImpl[F[_]: Functor](publisher: MetricsPublisher[F]) extends AdhocMetrics[F] {

  override val reset: F[Unit] = publisher.reset_adhoc.void

  override val report: F[Unit] = publisher.report_adhoc.void

  override def cheapSnapshot(tick: Tick): F[MetricsSnapshot] =
    publisher.cheap_snapshot(tick)
}
