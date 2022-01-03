package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import com.codahale.metrics.Counter
import com.github.chenharryhua.nanjin.guard.config.DigestedName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]: Functor] private[guard] (
  metricName: DigestedName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean) {

  private lazy val counter: Counter =
    eventPublisher.metricRegistry.counter(passThroughMRName(metricName, isCountAsError))

  def asError: NJBroker[F] = new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError = true)

  def passThrough[A: Encoder](a: A): F[Unit] =
    eventPublisher.passThrough(metricName, a.asJson, asError = isCountAsError).map(_ => counter.inc(1))

  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))
}
