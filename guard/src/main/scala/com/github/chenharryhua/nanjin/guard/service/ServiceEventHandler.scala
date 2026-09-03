package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Sync}
import cats.syntax.applicative.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.{ServiceParams, StackTrace}
import com.github.chenharryhua.nanjin.guard.event.Event.{ServicePanic, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StopReason}
import fs2.Stream
import fs2.concurrent.Channel

final private class ServiceEventHandler[F[_]: Sync] private (
  val serviceParams: ServiceParams,
  history: History[F, ServicePanic],
  channel: Channel[F, Event],
  logSink: LogSink[F]
) {
  private def publish(event: Event): F[Unit] =
    channel.send(event) >> logSink.write(event)

  def serviceStart(tick: Tick): F[Unit] =
    publish(
      ServiceStart(
        serviceParams.serviceIdentity,
        serviceParams.policies.restart.policy,
        serviceParams.brief,
        tick))

  def servicePanic(tick: Tick, stackTrace: StackTrace): F[Unit] = {
    val panic: ServicePanic = ServicePanic(
      serviceParams.serviceIdentity,
      serviceParams.policies.restart.policy,
      serviceParams.brief,
      tick,
      stackTrace)
    publish(panic) >> history.add(panic)
  }

  def serviceStop(cause: StopReason): F[Unit] =
    for {
      now <- serviceParams.serviceIdentity.timestamp[F]
      event = ServiceStop(
        serviceParams.serviceIdentity,
        serviceParams.policies.restart.policy,
        serviceParams.brief,
        now,
        cause)
      _ <- logSink.write(event)
      _ <- channel.closeWithElement(event)
    } yield ()

  // The isClosed check is best-effort, not atomic with serviceStop's close; correctness relies on
  // Channel.close being idempotent, so a concurrent close racing this guard is harmless.
  def serviceCancel: F[Unit] =
    channel.isClosed.ifM(().pure[F], serviceStop(StopReason.ByCancellation))

  def panicHistory: F[Vector[ServicePanic]] = history.value
}

private object ServiceEventHandler {
  def apply[F[_]: Async](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F]): Stream[F, ServiceEventHandler[F]] = {
    val history: F[History[F, ServicePanic]] =
      History[F, ServicePanic](serviceParams.history.map(_.panics))

    Stream.eval(history.map { panicHistory =>
      new ServiceEventHandler[F](
        serviceParams = serviceParams,
        history = panicHistory,
        channel = channel,
        logSink = logSink)
    })
  }
}
