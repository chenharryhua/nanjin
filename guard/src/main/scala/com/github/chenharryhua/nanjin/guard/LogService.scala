package com.github.chenharryhua.nanjin.guard

import cats.effect.Async
import io.circe.Encoder
import io.circe.generic.auto._
import io.circe.syntax._
import org.log4s.Logger

import scala.concurrent.duration.FiniteDuration

final private class LogService[F[_]] extends AlertService[F] {
  private val logger: Logger                                    = org.log4s.getLogger
  implicit private val errorEncoder: Encoder[Throwable]         = Encoder.encodeString.contramap(_.getMessage)
  implicit private val durationEncoder: Encoder[FiniteDuration] = Encoder.encodeString.contramap(_.toString())

  override def alert(status: Status)(implicit F: Async[F]): F[Unit] =
    status match {
      case ss @ ServiceStarted(_, _) =>
        F.blocking(logger.info(ss.asJson.noSpaces))
      case ss @ ServiceRestarting(_, _, _, error) =>
        F.blocking(logger.warn(error)(ss.asJson.noSpaces))
      case ss @ ServiceAbnormalStop(_, _, error) =>
        F.blocking(logger.error(error)(ss.asJson.noSpaces))
      case ss @ ServiceHealthCheck(_, _, _) =>
        F.blocking(logger.info(ss.asJson.noSpaces))
      case ss @ ActionRetrying(_, _, _, _, _, error, _) =>
        F.blocking(logger.warn(error)(ss.asJson.noSpaces))
      case ss @ ActionFailed(_, _, _, _, _, error, _) =>
        F.blocking(logger.error(error)(ss.asJson.noSpaces))
      case ss @ ActionSucced(_, _, _, _, _) =>
        F.blocking(logger.info(ss.asJson.noSpaces))
    }
}
