package com.github.chenharryhua.nanjin.guard.action

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{MeasurementID, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.NJEvent.PassThrough
import com.github.chenharryhua.nanjin.guard.event.{MetricCategory, MetricID, NJEvent}
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]: Monad: Clock] private[guard] (
  id: MeasurementID,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  isCounting: Boolean,
  dispatcher: Dispatcher[F]) {

  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(id, MetricCategory.PassThroughCounter).asJson.noSpaces)

  def withCounting: NJBroker[F] =
    new NJBroker[F](id, metricRegistry, channel, serviceParams, true, dispatcher)

  def passThrough[A: Encoder](a: A): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(PassThrough(id = id, timestamp = ts, serviceParams = serviceParams, value = a.asJson))
    } yield if (isCounting) counter.inc(1)

  def unsafePassThrough[A: Encoder](a: A): Unit =
    dispatcher.unsafeRunSync(passThrough(a))

}
