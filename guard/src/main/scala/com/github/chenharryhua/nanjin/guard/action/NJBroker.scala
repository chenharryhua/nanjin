package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.effect.std.Dispatcher
import cats.syntax.functor.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{CountAction, Digested, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]: Temporal] private[guard] (
  metricName: Digested,
  dispatcher: Dispatcher[F],
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isError: Boolean,
  isCounting: CountAction) {

  private val publisher: InstantEventPublisher[F] = new InstantEventPublisher[F](channel, serviceParams)

  private lazy val counter: Counter = metricRegistry.counter(passThroughMRName(metricName, isError))

  def asError: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, metricRegistry, channel, serviceParams, isError = true, isCounting)

  def withCounting: NJBroker[F] =
    new NJBroker[F](metricName, dispatcher, metricRegistry, channel, serviceParams, isError, CountAction.Yes)

  def passThrough[A: Encoder](a: A): F[Unit] =
    publisher.passThrough(metricName, a.asJson, asError = isError).map(_ => if (isCounting.value) counter.inc(1))

  def unsafePassThrough[A: Encoder](a: A): Unit = dispatcher.unsafeRunSync(passThrough(a))
}
