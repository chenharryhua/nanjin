package com.github.chenharryhua.nanjin.guard

import cats.{Functor, Semigroupal}
import cats.effect.kernel.Sync
import cats.syntax.apply.{catsSyntaxTuple2Semigroupal, catsSyntaxTuple5Semigroupal}
import cats.syntax.option.{catsSyntaxOptionId, none}
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Correlation, Domain, Message, StackTrace, Timestamp}
import io.circe.Encoder
import org.typelevel.log4cats.SelfAwareLogger

package object logging {

  private[logging] def get_alarm_level[F[_]: {Functor, Semigroupal}](
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

  private[logging] def create_reported_event[F[_], S: Encoder](
    serviceParams: ServiceParams,
    domain: Domain,
    message: S,
    level: AlarmLevel,
    stackTrace: Option[StackTrace])(implicit F: Sync[F]): F[ReportedEvent] =
    (F.unique, serviceParams.zonedNow).mapN { case (token, ts) =>
      ReportedEvent(
        serviceParams = serviceParams,
        domain = domain,
        timestamp = Timestamp(ts),
        correlation = Correlation(token),
        level = level,
        stackTrace = stackTrace,
        message = Message(Encoder[S].apply(message))
      )
    }
}
