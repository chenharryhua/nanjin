package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class Meter[F[_]](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean) {
  def asError: Meter[F] = new Meter[F](metricName, dispatcher, eventPublisher, isCountAsError = true)

  def mark(n: Long): F[Unit]    = eventPublisher.meterMark(metricName, n, isCountAsError)
  def unsafeMark(n: Long): Unit = dispatcher.unsafeRunSync(mark(n))
}
