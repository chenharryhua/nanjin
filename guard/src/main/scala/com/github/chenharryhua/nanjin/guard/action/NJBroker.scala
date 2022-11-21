package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Clock
import cats.syntax.all.*
import cats.Monad
import cats.effect.std.Dispatcher
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Digested, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.PassThrough
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]: Monad: Clock] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isError: Boolean,
  isCounting: Boolean,
  dispatcher: Dispatcher[F]) {

  private lazy val counter: Counter = metricRegistry.counter(passThroughMRName(digested, isError))

  def asError: NJBroker[F] =
    new NJBroker[F](digested, metricRegistry, channel, serviceParams, isError = true, isCounting, dispatcher)

  def withCounting: NJBroker[F] =
    new NJBroker[F](digested, metricRegistry, channel, serviceParams, isError, true, dispatcher)

  def passThrough[A: Encoder](a: A): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        PassThrough(
          digested = digested,
          timestamp = ts,
          serviceParams = serviceParams,
          isError = isError,
          value = a.asJson))
    } yield if (isCounting) counter.inc(1)

  def unsafePassThrough[A: Encoder](a: A): Unit =
    dispatcher.unsafeRunSync(passThrough(a))

}
