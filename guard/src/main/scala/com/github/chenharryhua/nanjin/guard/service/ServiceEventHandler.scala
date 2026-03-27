package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Sync}
import cats.effect.std.Console
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.flatMap.{catsSyntaxFlatMapOps, catsSyntaxIfM, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{ServicePanic, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StackTrace, StopReason, Timestamp}
import com.github.chenharryhua.nanjin.guard.service.logging.LogSink
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

  def service_start(tick: Tick): F[Unit] =
    publish(ServiceStart(serviceParams, tick))

  def service_panic(tick: Tick, stackTrace: StackTrace): F[Unit] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, stackTrace)
    publish(ServicePanic(serviceParams, tick, stackTrace)) >>
      history.add(panic)
  }

  def service_stop(cause: StopReason): F[Unit] =
    for {
      now <- serviceParams.zonedNow
      event = ServiceStop(serviceParams, Timestamp(now), cause)
      _ <- logSink.write(event)
      _ <- channel.closeWithElement(event)
    } yield ()

  def service_cancel: F[Unit] =
    channel.isClosed.ifM(().pure[F], service_stop(StopReason.ByCancellation))

  def panicHistory: F[Vector[ServicePanic]] = history.value
}

private object ServiceEventHandler {
  def apply[F[_]: {Async, Console}](
    serviceParams: ServiceParams,
    channel: Channel[F, Event]): Stream[F, ServiceEventHandler[F]] = {
    val history: F[History[F, ServicePanic]] =
      History[F, ServicePanic](serviceParams.historyCapacity.panic)

    Stream.eval((history, log_sink(serviceParams)).mapN { case (panicHistory, logSink) =>
      new ServiceEventHandler[F](
        serviceParams = serviceParams,
        history = panicHistory,
        channel = channel,
        logSink = logSink)
    })
  }
}
