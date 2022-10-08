package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Clock
import cats.Monad
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricReportType, MetricSnapshot, NJEvent}
import fs2.concurrent.Channel

final class NJMetrics[F[_]: Clock: Monad] private[service] (
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry) {

  def reset: F[Unit] = publisher.metricReset(channel, serviceParams, metricRegistry, None)

  private def reporting(metricFilter: MetricFilter): F[Unit] =
    publisher.metricReport(channel, serviceParams, metricRegistry, metricFilter, MetricReportType.Adhoc)

  def report(metricFilter: MetricFilter): F[Unit] = reporting(metricFilter)

  def deltaReport(metricFilter: MetricFilter): F[Unit] = reporting(metricFilter)

  def fullReport: F[Unit] = reporting(MetricFilter.ALL)

  // query
  def snapshot: MetricSnapshot =
    MetricSnapshot.full(metricRegistry, serviceParams)

  def snapshot(metricFilter: MetricFilter): MetricSnapshot =
    MetricSnapshot.regular(metricFilter, metricRegistry, serviceParams)

}
