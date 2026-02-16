package com.github.chenharryhua.nanjin

import cats.effect.implicits.genTemporalOps
import cats.effect.kernel.{Async, Sync}
import cats.syntax.applicativeError.catsSyntaxApplicativeError
import cats.syntax.flatMap.toFlatMapOps
import org.typelevel.log4cats.Logger

import scala.concurrent.duration.DurationInt

package object aws {
  private[aws] def shutdown[F[_]: Async](
    name: String,
    logger: Logger[F]
  )(shutdownEffect: F[Unit]): F[Unit] =
    shutdownEffect.timeout(10.seconds).attempt.flatMap {
      case Left(ex) => logger.warn(ex)(s"$name shutdown encountered an error")
      case Right(_) => logger.info(s"$name was closed")
    }

  private[aws] def blockingF[F[_], A](fa: => A, ctx: String, logger: Logger[F])(implicit F: Sync[F]): F[A] =
    F.blocking(fa).onError(ex => logger.error(ex)(ctx))
}
