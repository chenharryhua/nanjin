package com.github.chenharryhua.nanjin.aws

import cats.effect.kernel.Sync
import cats.syntax.applicativeError.given
import cats.syntax.flatMap.given
import org.typelevel.log4cats.Logger

private def shutdown[F[_]: Sync](
  name: String,
  logger: Logger[F]
)(close: => Unit): F[Unit] =
  Sync[F].blocking(close).attempt.flatMap {
    case Left(ex) => logger.warn(ex)(s"$name shutdown encountered an error")
    case Right(_) => logger.info(s"$name was closed")
  }

private def blockingF[F[_], A](fa: => A, ctx: String, logger: Logger[F])(using F: Sync[F]): F[A] =
  F.blocking(fa).onError(ex => logger.error(ex)(ctx))
