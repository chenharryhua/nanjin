package com.github.chenharryhua.nanjin.guard.service

import cats.data.Kleisli
import cats.effect.kernel.{Async, Sync}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{ServicePanic, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StackTrace, StopReason, Timestamp}
import fs2.Stream
import fs2.concurrent.Channel

final private class ServiceEventHandler[F[_]: Sync] private (
  val serviceParams: ServiceParams,
  history: History[F, ServicePanic],
  channel: Channel[F, Event],
  logSink: Kleisli[F, Event, Unit]
) {
  private def publish(event: Event): F[Unit] =
    channel.send(event) >> logSink.run(event)

  def serviceStart(tick: Tick): F[Unit] =
    publish(ServiceStart(serviceParams, tick))

  def servicePanic(tick: Tick, stackTrace: StackTrace): F[Unit] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, stackTrace)
    publish(ServicePanic(serviceParams, tick, stackTrace)) >>
      history.add(panic)
  }

  def serviceStop(cause: StopReason): F[Unit] =
    for {
      now <- serviceParams.zonedNow
      event = ServiceStop(serviceParams, Timestamp(now), cause)
      _ <- logSink.run(event)
      _ <- channel.closeWithElement(event)
    } yield ()

  def serviceCancel: F[Unit] =
    channel.isClosed.ifM(().pure[F], serviceStop(StopReason.ByCancellation))

  def panicHistory: F[Vector[ServicePanic]] = history.value
}

private object ServiceEventHandler {
  def apply[F[_]: Async](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: Kleisli[F, Event, Unit]): Stream[F, ServiceEventHandler[F]] = {
    val history: F[History[F, ServicePanic]] =
      History[F, ServicePanic](serviceParams.historyCapacity.panic)

    Stream.eval(history.map { panicHistory =>
      new ServiceEventHandler[F](
        serviceParams = serviceParams,
        history = panicHistory,
        channel = channel,
        logSink = logSink)
    })
  }
}
