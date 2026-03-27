package com.github.chenharryhua.nanjin.guard.service

import cats.effect.Async
import cats.effect.kernel.{Ref, Sync}
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Domain, Event, StackTrace}
import com.github.chenharryhua.nanjin.guard.service.History
import com.github.chenharryhua.nanjin.guard.service.logging.Log
import fs2.concurrent.Channel
import io.circe.Encoder

final private class ReportedEventHandler[F[_]](
  val alarmLevel: Ref[F, Option[AlarmLevel]],
  val errorHistory: History[F, ReportedEvent],
  serviceParams: ServiceParams,
  domain: Domain,
  alarmThreshold: AlarmLevel,
  channel: Channel[F, Event]
)(using F: Sync[F])
    extends Log[F] {

  def create(domain: Domain, alarmThreshold: AlarmLevel): Log[F] =
    new ReportedEventHandler[F](alarmLevel, errorHistory, serviceParams, domain, alarmThreshold, channel)

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
      errorHistory.add(event)
        .whenA(event.level === AlarmLevel.Error)

  // Combine dynamic alarmLevel with static alarmThreshold (floor).
  // Ensures Herald never emits below alarmThreshold, regardless of runtime logging level.
  override def enabled(level: AlarmLevel): F[Boolean] =
    alarmLevel.get.map(_.map(_.max(alarmThreshold)).exists(_ <= level))
}

private object ReportedEventHandler:
  def apply[F[_]: Async](
    serviceParams: ServiceParams,
    alarmLevel: AlarmLevel,
    channel: Channel[F, Event]): F[ReportedEventHandler[F]] = {
    val history = History[F, ReportedEvent](serviceParams.historyCapacity.error)

    val level = Ref.of[F, Option[AlarmLevel]](Some(alarmLevel))

    (history, level).mapN { (errorHistory, alarmLevel) =>
      new ReportedEventHandler(
        alarmLevel = alarmLevel,
        errorHistory = errorHistory,
        serviceParams = serviceParams,
        domain = Domain(serviceParams.serviceName.value),
        alarmThreshold = AlarmLevel.Error,
        channel = channel
      )
    }
  }
