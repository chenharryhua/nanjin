package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher

final class NJCounter[F[_]](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean
) {
  def asError: NJCounter[F] =
    new NJCounter[F](metricName, dispatcher, eventPublisher, isCountAsError = true)

  def increase(num: Long): F[Unit]    = eventPublisher.increase(metricName, num, asError = isCountAsError)
  def unsafeIncrease(num: Long): Unit = dispatcher.unsafeRunSync(increase(num))

  def replace(num: Long): F[Unit]    = eventPublisher.replace(metricName, num, asError = isCountAsError)
  def unsafeReplace(num: Long): Unit = dispatcher.unsafeRunSync(replace(num))

}
