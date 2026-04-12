package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import fs2.Stream

/** adhoc metrics report and reset
  */
trait AdhocMetrics[F[_]]:

  /** report current metrics
    */
  def report: F[Unit]

  def snapshots(
    f: Policy.type => Policy,
    g: ScrapeMode.type => ScrapeMode = _.Cheap): Stream[F, MetricsSnapshot]

  def meteredCounts(f: Policy.type => Policy): Stream[F, MeteredCounts]

end AdhocMetrics
