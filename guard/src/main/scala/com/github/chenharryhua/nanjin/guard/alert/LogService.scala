package com.github.chenharryhua.nanjin.guard.alert

import cats.effect.kernel.Sync
import cats.syntax.all.*
import com.github.chenharryhua.nanjin.guard.config.Severity
import org.log4s.Logger

final private class LogService[F[_]]()(implicit F: Sync[F]) extends AlertService[F] {
  private[this] val logger: Logger = org.log4s.getLogger

  override def alert(event: NJEvent): F[Unit] =
    event match {
      case _: ServiceStarted             => F.blocking(logger.info(event.show))
      case _: ServiceHealthCheck         => F.blocking(logger.info(event.show))
      case _: ServiceDailySummariesReset => F.blocking(logger.info(event.show))
      case _: ActionSucced               => F.blocking(logger.info(event.show))

      case _: PassThrough       => F.blocking(logger.info(event.show))
      case _: ActionQuasiSucced => F.blocking(logger.info(event.show))
      case _: ActionStart       => F.blocking(logger.info(event.show))
      case _: ServiceStopped    => F.blocking(logger.info(event.show))

      case ServicePanic(_, _, _, _, error)   => F.blocking(logger.warn(error.throwable)(event.show))
      case ActionRetrying(_, _, _, _, error) => F.blocking(logger.warn(error.throwable)(event.show))

      case ActionFailed(_, _, _, _, _, error) =>
        if (error.severity <= Severity.Error)
          F.blocking(logger.error(error.throwable)(event.show))
        else
          F.blocking(logger.warn(error.throwable)(event.show))

      case ForYourInformation(_, _, isError) =>
        if (isError)
          F.blocking(logger.error(new Exception("Reported Error"))(event.show))
        else
          F.blocking(logger.info(event.show))
    }
}

object LogService {
  def apply[F[_]: Sync]: AlertService[F] = new LogService[F]()
}
