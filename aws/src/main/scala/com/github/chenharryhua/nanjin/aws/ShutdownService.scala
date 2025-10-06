package com.github.chenharryhua.nanjin.aws

import cats.Applicative
import cats.effect.kernel.Resource.ExitCase
import cats.syntax.apply.*
import org.typelevel.log4cats.Logger

private trait ShutdownService[F[_]] {

  protected def closeService: F[Unit]

  final def shutdown(name: String, cause: ExitCase, logger: Logger[F])(implicit F: Applicative[F]): F[Unit] =
    cause match {
      case ExitCase.Succeeded  => logger.info(s"$name was closed normally") *> closeService
      case ExitCase.Errored(e) => logger.error(e)(s"$name was closed abnormally") *> closeService
      case ExitCase.Canceled   => logger.warn(s"$name was canceled") *> closeService
    }
}
