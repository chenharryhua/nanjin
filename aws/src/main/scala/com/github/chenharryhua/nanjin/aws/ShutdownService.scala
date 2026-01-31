package com.github.chenharryhua.nanjin.aws

import cats.effect.implicits.monadCancelOps_
import cats.effect.kernel.MonadCancelThrow
import cats.effect.kernel.Resource.ExitCase
import cats.implicits.{catsSyntaxApplicativeError, toFunctorOps}
import org.typelevel.log4cats.Logger

private trait ShutdownService[F[_]] {

  protected def closeService: F[Unit]

  /** Run a shutdown effect safely with logging.
    *
    * Guarantees:
    *   1. The main shutdown effect is always attempted, even if logging fails.
    *   2. Logging errors do not prevent shutdown.
    *   3. Shutdown errors are caught and ignored.
    *
    * @param name
    *   Resource name for logging
    * @param cause
    *   ExitCase from Resource.makeCase
    * @param logger
    *   Logger to report shutdown status
    */
  final def shutdown(
    name: String,
    cause: ExitCase,
    logger: Logger[F]
  )(implicit F: MonadCancelThrow[F]): F[Unit] = {
    val log: F[Unit] = cause match {
      case ExitCase.Succeeded  => logger.info(s"$name was closed normally")
      case ExitCase.Errored(e) => logger.error(e)(s"$name was closed abnormally")
      case ExitCase.Canceled   => logger.warn(s"$name was canceled")
    }

    log.attempt.guarantee(closeService.attempt.void).void
  }
}
