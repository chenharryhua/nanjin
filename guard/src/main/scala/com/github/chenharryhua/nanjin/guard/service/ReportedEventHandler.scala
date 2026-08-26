package com.github.chenharryhua.nanjin.guard.service

import cats.effect.Async
import cats.effect.kernel.{Ref, Sync}
import cats.syntax.applicative.given
import cats.syntax.apply.given
import cats.syntax.flatMap.given
import cats.syntax.functor.given
import cats.syntax.order.given
import com.github.chenharryhua.nanjin.common.logging.{Log, LogLevel}
import com.github.chenharryhua.nanjin.guard.config.{Domain, LogThreshold, ServiceParams, StackTrace}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Correlation, Event, Message}
import com.github.chenharryhua.nanjin.guard.service.History
import fs2.Stream
import fs2.concurrent.Channel
import io.circe.Encoder

final private class ReportedEventHandler[F[_]: Sync](
  val domain: Domain,
  val logThreshold: Ref[F, Option[LogThreshold]],
  history: History[F, ReportedEvent],
  serviceParams: ServiceParams,
  channel: Channel[F, Event],
  logSink: LogSink[F]
) {
  private def createReportedEvent[S: Encoder](message: S, level: LogLevel, stackTrace: Option[StackTrace])(
    using F: Sync[F]): F[ReportedEvent] =
    (F.unique, serviceParams.serviceIdentity.timestamp[F]).mapN { case (token, ts) =>
      ReportedEvent(
        serviceIdentity = serviceParams.serviceIdentity,
        timestamp = ts,
        domain = domain,
        correlation = Correlation(token),
        level = level,
        stackTrace = stackTrace,
        message = Message(Encoder[S].apply(message))
      )
    }

  def withDomain(domain: String): ReportedEventHandler[F] =
    new ReportedEventHandler[F](
      domain = Domain(domain),
      logThreshold = logThreshold,
      history = history,
      serviceParams = serviceParams,
      channel = channel,
      logSink = logSink)

  /** Logger that writes to the log sink and publishes to the event channel.
    *
    * The log sink write is gated by `logThreshold.logger`; the channel publication is gated by
    * `logThreshold.channel`. The `enabled` check uses the minimum of the two so that a message is constructed
    * whenever either path would accept it.
    */
  val logger: Log[F] = new Log[F] {
    override protected type M = ReportedEvent

    override protected def create[S: Encoder](
      message: S,
      level: LogLevel,
      stackTrace: Option[Throwable]): F[ReportedEvent] =
      createReportedEvent[S](message, level, stackTrace.map(StackTrace(_)))

    override protected def publish(event: ReportedEvent): F[Unit] =
      logThreshold.get.flatMap { threshold =>
        logSink.write(event).whenA(threshold.exists(_.logger <= event.level)) >>
          channel.send(event).whenA(threshold.exists(_.channel <= event.level)) >>
          history.add(event).whenA(event.level === LogLevel.Error)
      }

    override protected def enabled(level: LogLevel): F[Boolean] =
      logThreshold.get.map(_.exists(_.min <= level))
  }

  def errorHistory: F[Vector[ReportedEvent]] = history.value
}

private object ReportedEventHandler:
  def apply[F[_]: Async](
    serviceParams: ServiceParams,
    channel: Channel[F, Event],
    logSink: LogSink[F],
    logThreshold: LogThreshold
  ): Stream[F, ReportedEventHandler[F]] = {
    val history: F[History[F, ReportedEvent]] =
      History[F, ReportedEvent](serviceParams.history.map(_.errors))

    val initial: F[Ref[F, Option[LogThreshold]]] =
      Ref.of[F, Option[LogThreshold]](Some(logThreshold))

    val reh = (history, initial).mapN { (errorHistory, logThreshold) =>
      new ReportedEventHandler(
        domain = Domain("default"),
        logThreshold = logThreshold,
        history = errorHistory,
        serviceParams = serviceParams,
        channel = channel,
        logSink = logSink
      )
    }
    Stream.eval(reh)
  }
