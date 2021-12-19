package com.github.chenharryhua.nanjin.guard.action

import cats.Eval
import cats.effect.std.Dispatcher
import com.codahale.metrics.MetricFilter
import com.github.chenharryhua.nanjin.guard.event.{EventPublisher, MetricsSnapshot}

class Metrics[F[_]](dispatcher: Dispatcher[F], eventPublisher: EventPublisher[F]) {
  def reset: F[Unit]      = eventPublisher.metricsReset(MetricFilter.ALL, None)
  def unsafeReset(): Unit = dispatcher.unsafeRunSync(reset)
  def snapshot: Eval[MetricsSnapshot] =
    Eval.always(MetricsSnapshot(eventPublisher.metricRegistry, MetricFilter.ALL, eventPublisher.serviceInfo.params))
}
