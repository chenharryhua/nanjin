package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.Clock
import cats.Monad
import cats.implicits.{toFlatMapOps, toFunctorOps}
import com.codahale.metrics.{MetricFilter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.{MetricReportType, MetricSnapshot, NJEvent}
import fs2.concurrent.Channel

final class NJMetrics[F[_]: Clock: Monad] private[service] (
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  metricRegistry: MetricRegistry) {

  def reset: F[Unit] = builder.metricReset(serviceParams, metricRegistry, None).flatMap(channel.send).void

  private def reporting(metricFilter: MetricFilter): F[Unit] =
    builder
      .metricReport(serviceParams, metricRegistry, metricFilter, MetricReportType.Adhoc)
      .flatMap(channel.send)
      .void

  def report(metricFilter: MetricFilter): F[Unit] = reporting(metricFilter)
  def report: F[Unit]                             = reporting(MetricFilter.ALL)

  // query
  def snapshot: MetricSnapshot =
    MetricSnapshot.full(metricRegistry, serviceParams)

  def snapshot(metricFilter: MetricFilter): MetricSnapshot =
    MetricSnapshot.regular(metricFilter, metricRegistry, serviceParams)

}
