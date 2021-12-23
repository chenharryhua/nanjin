package com.github.chenharryhua.nanjin.guard.action

import cats.Functor
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
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

  private val name: String = passThroughMRName(metricName, isCountAsError)

  def asError: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError = true, countOrMeter)

  def withCount: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError, countOrMeter = true)

  def withMeter: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, eventPublisher, isCountAsError, countOrMeter = false)

  def passThrough[A: Encoder](a: A): F[Unit] =
    eventPublisher.passThrough(metricName, a.asJson, asError = isCountAsError).map { _ =>
      if (countOrMeter)
        eventPublisher.metricRegistry.counter(name).inc(1)
      else
        eventPublisher.metricRegistry.meter(name).mark(1)
    }

  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))
}
