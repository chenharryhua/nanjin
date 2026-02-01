package com.github.chenharryhua.nanjin

import cats.effect.implicits.{genTemporalOps, monadCancelOps_}
import cats.effect.kernel.{Async, Resource, Sync}
import cats.implicits.{catsSyntaxApplicativeError, toFunctorOps}
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.DurationInt

package object aws {
  private[aws] def shutdown[F[_]: Async](
    name: String,
    cause: Resource.ExitCase,
    logger: Logger[F]
  )(shutdownEffect: F[Unit]): F[Unit] = {

    val log: F[Unit] = cause match {
      case Resource.ExitCase.Succeeded  => logger.info(s"$name closed normally")
      case Resource.ExitCase.Errored(e) => logger.error(e)(s"$name closed abnormally")
      case Resource.ExitCase.Canceled   => logger.warn(s"$name shutdown canceled")
    }
    val close: F[Unit] = shutdownEffect.timeout(10.seconds).handleErrorWith { ex =>
      logger.warn(ex)(s"$name shutdown encountered an error")
    }

    log.attempt.guarantee(close).void
  }

  private[aws] def blockingF[F[_], A](fa: => A, ctx: String, logger: Logger[F])(implicit F: Sync[F]): F[A] =
    F.blocking(fa).onError(ex => logger.error(ex)(ctx))
}
