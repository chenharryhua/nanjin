package com.github.chenharryhua.nanjin.guard

import cats.syntax.apply.given
import cats.syntax.option.{none, given}
import cats.{Functor, Semigroupal}
import com.github.chenharryhua.nanjin.common.logging.LogLevel
import org.typelevel.log4cats.SelfAwareLogger

package object service {
  private[service] def getLogLevel[F[_]: {Functor, Semigroupal}](
    log: SelfAwareLogger[F]): F[Option[LogLevel]] =
    (log.isTraceEnabled, log.isDebugEnabled, log.isInfoEnabled, log.isWarnEnabled, log.isErrorEnabled)
      .mapN { case (trace, debug, info, warn, error) =>
        if (trace) LogLevel.Debug.some
        else if (debug) LogLevel.Debug.some
        else if (info) LogLevel.Info.some
        else if (warn) LogLevel.Warn.some
        else if (error) LogLevel.Error.some
        else none[LogLevel]
      }

}
