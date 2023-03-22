package com.github.chenharryhua.nanjin.guard.action

import cats.Monad
import cats.effect.kernel.Clock
import cats.effect.std.Dispatcher
import cats.syntax.all.*
import com.codahale.metrics.{Counter, MetricRegistry}
import com.github.chenharryhua.nanjin.guard.config.{
  Category,
  CounterKind,
  MetricID,
  MetricName,
  ServiceParams
}
import com.github.chenharryhua.nanjin.guard.event.NJEvent
import com.github.chenharryhua.nanjin.guard.event.NJEvent.PassThrough
import fs2.concurrent.Channel
import io.circe.Encoder
import io.circe.syntax.*

final class NJBroker[F[_]: Monad: Clock] private[guard] (
  name: MetricName,
  metricRegistry: MetricRegistry,
  channel: Channel[F, NJEvent],
  serviceParams: ServiceParams,
  dispatcher: Dispatcher[F],
  isCounting: Boolean) {
  private lazy val counter: Counter =
    metricRegistry.counter(MetricID(name, Category.Counter(Some(CounterKind.PassThrough))).asJson.noSpaces)

  def withCounting: NJBroker[F] =
    new NJBroker[F](name, metricRegistry, channel, serviceParams, dispatcher, true)

  def passThrough[A: Encoder](a: A): F[Unit] =
    for {
      ts <- serviceParams.zonedNow
      _ <- channel.send(
        PassThrough(metricName = name, timestamp = ts, serviceParams = serviceParams, value = a.asJson))
    } yield if (isCounting) counter.inc(1)

  def unsafePassThrough[A: Encoder](a: A): Unit =
    dispatcher.unsafeRunSync(passThrough(a))

}
