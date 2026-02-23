package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import cats.syntax.apply.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Event.ReportedEvent
import com.github.chenharryhua.nanjin.guard.event.{Correlation, Domain, Message, StackTrace, Timestamp}
import io.circe.Encoder

package object logging {

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
