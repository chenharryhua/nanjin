package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.Sync
import cats.syntax.all._
import org.log4s.Logger

final private class LogService[F[_]]()(implicit F: Sync[F]) extends AlertService[F] {
  private val logger: Logger = org.log4s.getLogger

  override def alert(event: NJEvent): F[Unit] =
    event match {
      case _: ServiceStarted             => F.blocking(logger.info(event.show))
      case _: ServiceHealthCheck         => F.blocking(logger.info(event.show))
      case _: ServiceDailySummariesReset => F.blocking(logger.info(event.show))
      case _: ActionSucced               => F.blocking(logger.info(event.show))
      case _: ForYourInformation         => F.blocking(logger.info(event.show))
      case _: PassThrough                => F.blocking(logger.info(event.show))
      case _: ActionQuasiSucced          => F.blocking(logger.info(event.show))
      case _: ActionStart                => F.blocking(logger.info(event.show))

      case ServicePanic(_, _, _, _, error)   => F.blocking(logger.warn(error.throwable)(event.show))
      case ActionRetrying(_, _, _, _, error) => F.blocking(logger.warn(error.throwable)(event.show))

      case ServiceStopped(_, _, params) =>
        if (params.isNormalStop)
          F.blocking(logger.info(event.show))
        else
          F.blocking(logger.error(new Exception("service was abnormally stopped"))(event.show))
      case ActionFailed(_, _, _, _, _, error) => F.blocking(logger.error(error.throwable)(event.show))
    }
}

object LogService {
  def apply[F[_]: Sync]: AlertService[F] = new LogService[F]()
}
