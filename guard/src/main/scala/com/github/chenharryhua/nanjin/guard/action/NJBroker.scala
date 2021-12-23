package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean) {

  def asError: NJBroker[F] = new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError = true)

  def passThrough[A: Encoder](a: A): F[Unit] =
    eventPublisher.passThrough(metricName, a.asJson, asError = isCountAsError)
  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))

}
