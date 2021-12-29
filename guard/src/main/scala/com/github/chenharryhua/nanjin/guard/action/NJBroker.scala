package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import com.codahale.metrics.{Counter, Meter}
import com.github.chenharryhua.nanjin.guard.config.DigestedName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher
import io.circe.Encoder
import io.circe.syntax.*

/** countOrMeter: default meter
  */
final class NJBroker[F[_]: Functor](
  name: DigestedName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean,
  counterOrMeter: Boolean) {

  private val mrMame: String = passThroughMRName(name, isCountAsError, counterOrMeter)
  private lazy val cm: Either[Counter, Meter] =
    if (counterOrMeter) Left(eventPublisher.metricRegistry.counter(mrMame))
    else Right(eventPublisher.metricRegistry.meter(mrMame))

  def asError: NJBroker[F] =
    new NJBroker[F](name, dispatcher, eventPublisher, isCountAsError = true, counterOrMeter)

  def withCounter: NJBroker[F] =
    new NJBroker[F](name, dispatcher, eventPublisher, isCountAsError, counterOrMeter = true)

  def withMeter: NJBroker[F] =
    new NJBroker[F](name, dispatcher, eventPublisher, isCountAsError, counterOrMeter = false)

  def passThrough[A: Encoder](a: A): F[Unit] =
    eventPublisher.passThrough(name, a.asJson, asError = isCountAsError).map { _ =>
      cm.fold(_.inc(1), _.mark(1))
    }

  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))
}
