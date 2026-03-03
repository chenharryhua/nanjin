package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import cats.effect.std.Console
import cats.syntax.apply.catsSyntaxTuple5Semigroupal
import cats.syntax.option.{catsSyntaxOptionId, none}
import cats.{Functor, Semigroupal}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.logging.LogSink
import org.typelevel.log4cats.{LoggerName, SelfAwareLogger}

package object service {

  private[service] def get_alarm_level[F[_]: Functor: Semigroupal](
    log: SelfAwareLogger[F]): F[Option[AlarmLevel]] =
    (log.isTraceEnabled, log.isDebugEnabled, log.isInfoEnabled, log.isWarnEnabled, log.isErrorEnabled).mapN {
      case (trace, debug, info, warn, error) =>
        if (trace) AlarmLevel.Debug.some
        else if (debug) AlarmLevel.Debug.some
        else if (info) AlarmLevel.Info.some
        else if (warn) AlarmLevel.Warn.some
        else if (error) AlarmLevel.Error.some
        else none[AlarmLevel]
    }

  private[service] def log_sink[F[_]: Sync: Console](serviceParams: ServiceParams): F[LogSink[F]] =
    LogSink[F](serviceParams.logFormat, serviceParams.zoneId, LoggerName(serviceParams.serviceName.value))

}
