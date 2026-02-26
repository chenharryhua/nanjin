package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.{Ref, Sync}
import cats.effect.std.AtomicCell
import cats.syntax.applicative.catsSyntaxApplicativeByName
import cats.syntax.eq.catsSyntaxEq
import cats.syntax.flatMap.catsSyntaxFlatMapOps
import cats.syntax.functor.toFunctorOps
import cats.syntax.order.{catsSyntaxOrder, catsSyntaxPartialOrder}
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
    alarmLevel: Ref[F, Option[AlarmLevel]],
    alarmThreshold: AlarmLevel,
    channel: Channel[F, Event],
    errorHistory: AtomicCell[F, CircularFifoQueue[ReportedEvent]]): Log[F] =
    new HeraldImpl[F](serviceParams, domain, alarmLevel, alarmThreshold, channel, errorHistory)

  final private class HeraldImpl[F[_]](
    serviceParams: ServiceParams,
    domain: Domain,
    alarmLevel: Ref[F, Option[AlarmLevel]],
    alarmThreshold: AlarmLevel,
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

    override def publish(event: ReportedEvent): F[Unit] =
      channel.send(event) >>
        errorHistory.modify(queue => (queue, queue.add(event)))
          .whenA(event.level === AlarmLevel.Error)

    // Combine dynamic alarmLevel with static alarmThreshold (floor).
    // Ensures Herald never emits below alarmThreshold, regardless of runtime logging level.
    override def enabled(level: AlarmLevel): F[Boolean] =
      alarmLevel.get.map(_.map(_.max(alarmThreshold)).exists(_ <= level))
  }
}
