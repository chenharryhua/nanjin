package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class Counter[F[_]](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F]
) {
  def count(num: Long): F[Unit]    = eventPublisher.count(metricName, num)
  def unsafeCount(num: Long): Unit = dispatcher.unsafeRunSync(count(num))
}
