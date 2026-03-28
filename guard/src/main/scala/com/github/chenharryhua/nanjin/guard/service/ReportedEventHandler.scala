package com.github.chenharryhua.nanjin.guard.service

import cats.effect.Async
import cats.effect.kernel.{Ref, Sync}
import cats.effect.std.Console
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Correlation, Domain, Event, Message, StackTrace, Timestamp}
import com.github.chenharryhua.nanjin.guard.service.History
import com.github.chenharryhua.nanjin.guard.service.logging.{Log, LogSink}
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Encoder
import org.typelevel.log4cats.LoggerName

final private class ReportedEventHandler[F[_]: Console](
  val domain: Domain,
  val alarmThreshold: Ref[F, Option[AlarmLevel]],
  history: History[F, ReportedEvent],
  serviceParams: ServiceParams,
  channel: Channel[F, Event]
)(using F: Sync[F]) {
  private def create_reported_event[S: Encoder](
    message: S,
    level: AlarmLevel,
    stackTrace: Option[StackTrace])(using F: Sync[F]): F[ReportedEvent] =
    (F.unique, serviceParams.zonedNow).mapN { case (token, ts) =>
      ReportedEvent(
        serviceParams = serviceParams,
        domain = domain,
        timestamp = Timestamp(ts),
        correlation = Correlation(token),
        level = level,
        stackTrace = stackTrace,
        message = Message(Encoder[S].apply(message))
      )
    }

  def withDomain(name: String): ReportedEventHandler[F] =
    new ReportedEventHandler[F](
      domain = Domain(name),
      alarmThreshold = alarmThreshold,
      history = history,
      serviceParams = serviceParams,
      channel = channel)

  def spawnHerald : Log[F] =
    new Log[F] {
      override def create[S: Encoder](
        message: S,
        level: AlarmLevel,
        stackTrace: Option[StackTrace]): F[ReportedEvent] =
        create_reported_event[S](message, level, stackTrace)

      override def publish(event: ReportedEvent): F[Unit] =
        channel.send(event) >>
          history.add(event).whenA(event.level === AlarmLevel.Error)

      // Combine dynamic alarmLevel with static threshold (floor).
      // Ensures Herald never emits below threshold, regardless of runtime logging level.
      override def enabled(level: AlarmLevel): F[Boolean] =
        alarmThreshold.get.map(_.exists(_ <= level))
    }

  def spawnLogger(loggerName: LoggerName): Log[F] =
    new Log[F] {
      private val logSink = LogSink[F](serviceParams.logFormat, serviceParams.zoneId, loggerName)

      override def create[S: Encoder](
        message: S,
        level: AlarmLevel,
        stackTrace: Option[StackTrace]): F[ReportedEvent] =
        create_reported_event[S](message, level, stackTrace)

      override def publish(event: ReportedEvent): F[Unit] =
        logSink.write(event)

      override def enabled(level: AlarmLevel): F[Boolean] =
        alarmThreshold.get.map(_.exists(_ <= level))

    }

  def errorHistory: F[Vector[ReportedEvent]] = history.value

}

private object ReportedEventHandler:
  def apply[F[_]: {Async, Console}](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    alarmLevel: AlarmLevel
  ): Stream[F, ReportedEventHandler[F]] = {
    val history: F[History[F, ReportedEvent]] =
      History[F, ReportedEvent](serviceParams.historyCapacity.error)

    val initial: F[Ref[F, Option[AlarmLevel]]] =
      Ref.of[F, Option[AlarmLevel]](Some(alarmLevel))

    val re = (history, initial).mapN { (errorHistory, alarmThreshold) =>
      new ReportedEventHandler(
        domain = Domain(serviceParams.serviceName.value),
        alarmThreshold = alarmThreshold,
        history = errorHistory,
        serviceParams = serviceParams,
        channel = channel
      )
    }
    Stream.eval(re)
  }
