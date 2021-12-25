package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import com.codahale.metrics.{Counter, Meter}
import com.github.chenharryhua.nanjin.guard.config.MetricName
import com.github.chenharryhua.nanjin.guard.event.EventPublisher
import io.circe.Encoder
import io.circe.syntax.*

/** countOrMeter: default meter
  */
final class NJBroker[F[_]: Functor](
  metricName: MetricName,
  dispatcher: Dispatcher[F],
  eventPublisher: EventPublisher[F],
  isCountAsError: Boolean,
  countOrMeter: Boolean) {

  private val name: String = passThroughMRName(metricName, isCountAsError, countOrMeter)
  private lazy val cm: Either[Counter, Meter] =
    if (countOrMeter) Left(eventPublisher.metricRegistry.counter(name))
    else Right(eventPublisher.metricRegistry.meter(name))

  def asError: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError = true, countOrMeter)

  def withCount: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError, countOrMeter = true)

  def withMeter: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError, countOrMeter = false)

  def passThrough[A: Encoder](a: A): F[Unit] =
    eventPublisher.passThrough(metricName, a.asJson, asError = isCountAsError).map { _ =>
      cm.fold(_.inc(1), _.mark(1))
    }

  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))
}
