package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot

/** adhoc metrics report and reset
  */
trait AdhocMetrics[F[_]] {

  /** reset all counters to zero
    */
  def reset: F[Unit]

  /** report current metrics
    */
  def report: F[Unit]

  def cheapSnapshot(tick: Tick): F[MetricsSnapshot]
}
