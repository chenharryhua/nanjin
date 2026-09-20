package com.github.chenharryhua.nanjin.guard.service

import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.guard.event.Event.MetricsSnapshot
import com.github.chenharryhua.nanjin.guard.metrics.snapshot.{MeteredCounts, ScrapeMode}
import fs2.Stream

/** Ad-hoc metrics reporting and streaming access to metric snapshots.
  *
  * Accessible via `agent.adhoc`. Provides on-demand reporting and scheduled streaming of metrics data
  * independent of the service's periodic report policy.
  */
trait AdhocReport[F[_]]:

  /** Trigger an immediate metrics snapshot and publish it as an event. */
  def report: F[Unit]

  /** Stream periodic metrics snapshots on a custom schedule.
    *
    * @param f
    *   policy controlling the snapshot frequency
    * @param g
    *   scrape mode (Cheap skips gauges, Full includes all)
    */
  def snapshots(
    f: Policy.type => Policy,
    g: ScrapeMode.type => ScrapeMode = _.Cheap): Stream[F, MetricsSnapshot]

  /** Stream metered count deltas (meters and timers) on a custom schedule.
    *
    * Each emission contains the delta since the previous emission, suitable for feeding into observers like
    * CloudWatch.
    *
    * @param f
    *   policy controlling the emission frequency
    */
  def meteredCounts(f: Policy.type => Policy): Stream[F, MeteredCounts]

end AdhocReport
