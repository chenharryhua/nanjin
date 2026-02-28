package com.github.chenharryhua.nanjin.guard.logging

import cats.effect.kernel.{Ref, Sync}
import cats.syntax.functor.toFunctorOps
import cats.syntax.order.catsSyntaxPartialOrder
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Domain, StackTrace}
import io.circe.Encoder

object Logger {
  def apply[F[_]: Sync](
    serviceParams: ServiceParams,
    domain: Domain,
    alarmLevel: Ref[F, Option[AlarmLevel]],
    logSink: LogSink[F]): Log[F] =
    new LogPublisher[F](serviceParams, domain, alarmLevel, logSink)

  final private class LogPublisher[F[_]](
    serviceParams: ServiceParams,
    domain: Domain,
    alarmLevel: Ref[F, Option[AlarmLevel]],
    logSink: LogSink[F])(implicit F: Sync[F])
      extends Log[F] {

    override def create[S: Encoder](
      message: S,
      level: AlarmLevel,
      stackTrace: Option[StackTrace]): F[ReportedEvent] =
      create_reported_event[F, S](serviceParams, domain, message, level, stackTrace)

    override def publish(event: ReportedEvent): F[Unit] =
      logSink.write(event)

    override def enabled(level: AlarmLevel): F[Boolean] =
      alarmLevel.get.map(_.exists(_ <= level))
  }
}
