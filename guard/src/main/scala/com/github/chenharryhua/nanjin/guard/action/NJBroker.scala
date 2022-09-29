package com.github.chenharryhua.nanjin.guard.action

import cats.effect.kernel.Temporal
import cats.syntax.functor.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{Digested, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]: Temporal] private[guard] (
  digested: Digested,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isError: Boolean,
  isCounting: Boolean) {

  private lazy val counter: Counter = metricRegistry.counter(passThroughMRName(digested, isError))

  def asError: NJBroker[F] =
    new NJBroker[F](digested, metricRegistry, channel, serviceParams, isError = true, isCounting)

  def withCounting: NJBroker[F] =
    new NJBroker[F](digested, metricRegistry, channel, serviceParams, isError, true)

  def passThrough[A: Encoder](a: A): F[Unit] =
    publisher
      .passThrough(channel, serviceParams, digested, a.asJson, isError)
      .map(_ => if (isCounting) counter.inc(1))

}
