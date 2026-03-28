package com.github.chenharryhua.nanjin.guard

import cats.syntax.apply.given
import cats.syntax.option.{none, given}
import cats.{Functor, Semigroupal}
import com.github.chenharryhua.nanjin.guard.config.AlarmLevel
import org.typelevel.log4cats.SelfAwareLogger

package object service {
  private[service] def get_alarm_level[F[_]: {Functor, Semigroupal}](
    log: SelfAwareLogger[F]): F[Option[AlarmLevel]] =
    (log.isTraceEnabled, log.isDebugEnabled, log.isInfoEnabled, log.isWarnEnabled, log.isErrorEnabled)
      .mapN { case (trace, debug, info, warn, error) =>
        if (trace) AlarmLevel.Debug.some
        else if (debug) AlarmLevel.Debug.some
        else if (info) AlarmLevel.Info.some
        else if (warn) AlarmLevel.Warn.some
        else if (error) AlarmLevel.Error.some
        else none[AlarmLevel]
      }

}
