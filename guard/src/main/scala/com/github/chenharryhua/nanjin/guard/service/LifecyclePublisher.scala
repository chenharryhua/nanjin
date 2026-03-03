package com.github.chenharryhua.nanjin.guard.service

import cats.effect.kernel.{Async, Sync}
import cats.effect.std.{AtomicCell, Console}
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import cats.syntax.flatMap.{catsSyntaxFlatMapOps, catsSyntaxIfM, toFlatMapOps}
import cats.syntax.functor.toFunctorOps
import com.github.chenharryhua.nanjin.common.chrono.Tick
import com.github.chenharryhua.nanjin.guard.config.ServiceParams
import com.github.chenharryhua.nanjin.guard.event.Event.{ServicePanic, ServiceStart, ServiceStop}
import com.github.chenharryhua.nanjin.guard.event.{Event, StackTrace, StopReason, Timestamp}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import fs2.Stream
import fs2.concurrent.Channel
import org.apache.commons.collections4.queue.CircularFifoQueue

import scala.jdk.CollectionConverters.IteratorHasAsScala

final private class LifecyclePublisher[F[_]: Sync] private (
  panicHistory: AtomicCell[F, CircularFifoQueue[ServicePanic]],
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  logSink: LogSink[F]
) {
  private def publish(event: Event): F[Unit] =
    channel.send(event) >> logSink.write(event)

  def service_start(tick: Tick): F[Unit] =
    publish(ServiceStart(serviceParams, tick))

  def service_panic(tick: Tick, stackTrace: StackTrace): F[Unit] = {
    val panic: ServicePanic = ServicePanic(serviceParams, tick, stackTrace)
    publish(ServicePanic(serviceParams, tick, stackTrace)).flatTap { _ =>
      panicHistory.modify(queue => queue -> queue.add(panic))
    }
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

  def get_panic_history: F[List[ServicePanic]] =
    panicHistory.get.map(_.iterator().asScala.toList)
}

private object LifecyclePublisher {
  def apply[F[_]: Async: Console](
    serviceParams: ServiceParams,
    channel: Channel[F, Event]): Stream[F, LifecyclePublisher[F]] = {
    val cell: F[AtomicCell[F, CircularFifoQueue[ServicePanic]]] =
      AtomicCell[F].of(new CircularFifoQueue[ServicePanic](serviceParams.historyCapacity.panic))

    Stream.eval((cell, log_sink(serviceParams)).mapN { case (a, b) =>
      new LifecyclePublisher[F](a, serviceParams, channel, b)
    })
  }
}
