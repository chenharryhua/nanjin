package com.github.chenharryhua.nanjin.guard.action

import cats.Eval
import cats.effect.std.Dispatcher
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.guard.event.{EventPublisher, MetricReportType, MetricsSnapshot}

class NJMetrics[F[_]](dispatcher: Dispatcher[F], eventPublisher: EventPublisher[F], metricFilter: MetricFilter) {
  def withMetricFilter(metricFilter: MetricFilter): NJMetrics[F] =
    new NJMetrics[F](dispatcher, eventPublisher, metricFilter)

  def reset: F[Unit]      = eventPublisher.metricsReset(metricFilter, None)
  def unsafeReset(): Unit = dispatcher.unsafeRunSync(reset)

  val snapshot: Eval[MetricsSnapshot] =
    Eval.always(MetricsSnapshot(eventPublisher.metricRegistry, metricFilter, eventPublisher.serviceInfo.serviceParams))

  def report: F[Unit]      = eventPublisher.metricsReport(metricFilter, MetricReportType.AdventiveReport)
  def unsafeReport(): Unit = dispatcher.unsafeRunSync(report)
}
