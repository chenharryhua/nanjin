package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.Sync
import cats.effect.std.AtomicCell
import cats.syntax.applicative.catsSyntaxApplicativeId
import cats.syntax.apply.catsSyntaxApplyOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.order.catsSyntaxPartialOrder
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event, StackTrace}
import fs2.concurrent.Channel
import io.circe.Encoder
import org.apache.commons.collections4.queue.CircularFifoQueue

object Herald {
  def apply[F[_]: Sync](
    serviceParams: ServiceParams,
    domain: Domain,
    channel: Channel[F, Event],
    errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]]): Log[F] =
    new HeraldImpl[F](serviceParams, domain, channel, errorHistory)

  final private class HeraldImpl[F[_]](
    serviceParams: ServiceParams,
    domain: Domain,
    channel: Channel[F, Event],
    errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]])(implicit F: Sync[F])
      extends Log[F] {

    override def create[S: Encoder](
      message: S,
      level: AlarmLevel,
      stackTrace: Option[StackTrace]): F[ReportedEvent] =
      create_reported_event[F, S](
        serviceParams = serviceParams,
        domain = domain,
        message = message,
        level = level,
        stackTrace = stackTrace)

    override def publish(event: ReportedEvent): F[Unit] = event.level match {
      case AlarmLevel.Error =>
        channel.send(event) *> errorHistory.modify(queue => (queue, queue.add(event))).void
      case AlarmLevel.Warn  => channel.send(event).void
      case AlarmLevel.Good  => channel.send(event).void
      case AlarmLevel.Info  => channel.send(event).void
      case AlarmLevel.Debug => F.unit
    }

    override def enabled(level: AlarmLevel): F[Boolean] =
      (level >= AlarmLevel.Info).pure[F]
  }
}
