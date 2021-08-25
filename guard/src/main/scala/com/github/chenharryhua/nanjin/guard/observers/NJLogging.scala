package com.github.chenharryhua.nanjin.guard.observers

import cats.data.Reader
import cats.effect.kernel.Sync
import cats.implicits.toShow
import com.github.chenharryhua.nanjin.guard.event.{ActionFailed, ActionRetrying, NJEvent, ServicePanic}
import fs2.{INothing, Pipe, Stream}
import io.circe.syntax.EncoderOps
import org.log4s.Logger

object logging {
  private val default: EventFilter = EventFilter(
    serviceStart = true,
    servicePanic = true,
    serviceStop = true,
    actionSucc = true,
    actionRetry = true,
    actionFirstRetry = false,
    actionStart = true,
    actionFailure = true,
    fyi = true,
    passThrough = true,
    mrReport = true,
    sampling = 1
  )

  def json[F[_]: Sync]: NJLogging[F] = new NJLogging[F](Reader(_.asJson.noSpaces), default)
  def text[F[_]: Sync]: NJLogging[F] = new NJLogging[F](Reader(_.show), default)
}

final class NJLogging[F[_]](converter: Reader[NJEvent, String], eventFilter: EventFilter)(implicit F: Sync[F])
    extends Pipe[F, NJEvent, INothing] {

  private def updateEventFilter(f: EventFilter => EventFilter): NJLogging[F] =
    new NJLogging[F](converter, f(eventFilter))

  def blockSucc: NJLogging[F]        = updateEventFilter(_.copy(actionSucc = false))
  def blockStart: NJLogging[F]       = updateEventFilter(_.copy(actionStart = false))
  def blockRetry: NJLogging[F]       = updateEventFilter(_.copy(actionRetry = false))
  def blockReport: NJLogging[F]      = updateEventFilter(_.copy(mrReport = false))
  def blockFyi: NJLogging[F]         = updateEventFilter(_.copy(fyi = false))
  def blockPassThrough: NJLogging[F] = updateEventFilter(_.copy(passThrough = false))

  private[this] val logger: Logger = org.log4s.getLogger

  override def apply(events: Stream[F, NJEvent]): Stream[F, INothing] =
    events
      .filter(eventFilter)
      .evalMap { event =>
        val out: String = converter(event)
        event match {
          case ServicePanic(_, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out)))
          case ActionRetrying(_, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.warn(out))(ex => logger.warn(ex)(out)))
          case ActionFailed(_, _, _, _, _, error) =>
            F.blocking(error.throwable.fold(logger.error(out))(ex => logger.error(ex)(out)))
          case _ => F.blocking(logger.info(out))
        }
      }
      .drain

}
