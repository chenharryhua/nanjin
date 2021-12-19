package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class Counter[F[_]](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F]
) {
  def increase(num: Long): F[Unit]    = eventPublisher.increase(metricName, num)
  def unsafeIncrease(num: Long): Unit = dispatcher.unsafeRunSync(increase(num))

  def replace(num: Long): F[Unit]    = eventPublisher.replace(metricName, num)
  def unsafeReplace(num: Long): Unit = dispatcher.unsafeRunSync(replace(num))
}
