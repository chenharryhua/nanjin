package com.github.chenharryhua.nanjin.guard.action

import cats.effect.std.Dispatcher
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher
import io.circe.Encoder
import io.circe.syntax.*

final class Broker[F[_]](metricName: MetricName, dispatcher: Dispatcher[F], eventPublisher: EventPublisher[F]) {
  def passThrough[A: Encoder](a: A): F[Unit]    = eventPublisher.passThrough(metricName, a.asJson)
  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))
}
