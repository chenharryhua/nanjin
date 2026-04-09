package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot

/** adhoc metrics report and reset
  */
trait AdhocMetrics[F[_]]:

  /** report current metrics
    */
  def report: F[Unit]

  def snapshot(tick: Tick, f: ScrapeMode.type => ScrapeMode): F[MetricsSnapshot]

end AdhocMetrics
