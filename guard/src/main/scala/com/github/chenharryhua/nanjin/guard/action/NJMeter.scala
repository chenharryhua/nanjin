package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class NJMeter[F[_]](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean) {
  def asError: NJMeter[F] = new NJMeter[F](metricName, dispatcher, eventPublisher, isCountAsError = true)

  def mark(n: Long): F[Unit]    = eventPublisher.meterMark(metricName, n, isCountAsError)
  def unsafeMark(n: Long): Unit = dispatcher.unsafeRunSync(mark(n))
}
