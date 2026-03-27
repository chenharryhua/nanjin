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
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Encoder

final private class ReportedEventHandler[F[_]](
  val domain: Domain,
  val alarmLevel: Ref[F, Option[AlarmLevel]],
  history: History[F, ReportedEvent],
  serviceParams: ServiceParams,
  alarmThreshold: AlarmLevel,
  channel: Channel[F, Event]
)(using F: Sync[F])
    extends Log[F] {
  def withDomain(name: String): ReportedEventHandler[F] =
    new ReportedEventHandler[F](
      domain = Domain(name),
      alarmLevel = alarmLevel,
      history = history,
      serviceParams = serviceParams,
      alarmThreshold = alarmThreshold,
      channel = channel)

  def createLog(threshold: AlarmLevel): Log[F] =
    new ReportedEventHandler[F](
      domain = domain,
      alarmLevel = alarmLevel,
      history = history,
      serviceParams = serviceParams,
      alarmThreshold = threshold,
      channel = channel)
      
  def errorHistory: F[Vector[ReportedEvent]] = history.value

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
      history.add(event).whenA(event.level === AlarmLevel.Error)

  // Combine dynamic alarmLevel with static alarmThreshold (floor).
  // Ensures Herald never emits below alarmThreshold, regardless of runtime logging level.
  override def enabled(level: AlarmLevel): F[Boolean] =
    alarmLevel.get.map(_.map(_.max(alarmThreshold)).exists(_ <= level))
}

private object ReportedEventHandler:
  def apply[F[_]: Async](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    alarmLevel: AlarmLevel
  ): Stream[F, ReportedEventHandler[F]] = {
    val history: F[History[F, ReportedEvent]] =
      History[F, ReportedEvent](serviceParams.historyCapacity.error)

    val initial: F[Ref[F, Option[AlarmLevel]]] =
      Ref.of[F, Option[AlarmLevel]](Some(alarmLevel))

    val re = (history, initial).mapN { (errorHistory, alarmLevel) =>
      new ReportedEventHandler(
        domain = Domain(serviceParams.serviceName.value),
        alarmLevel = alarmLevel,
        history = errorHistory,
        serviceParams = serviceParams,
        alarmThreshold = AlarmLevel.Error,
        channel = channel
      )
    }
    Stream.eval(re)
  }
