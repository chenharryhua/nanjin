package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Sync
import cats.implicits.catsSyntaxTuple2Semigroupal
import com.github.chenharryhua.nanjin.guard.config.{AlarmLevel, ServiceParams}
import com.github.chenharryhua.nanjin.guard.event.Error
import com.github.chenharryhua.nanjin.guard.event.Event.ServiceMessage
import io.circe.Encoder

package object action {

  private[action] def toServiceMessage[F[_], S: Encoder](
    serviceParams: ServiceParams,
    msg: S,
    level: AlarmLevel,
    error: Option[Error])(implicit F: Sync[F]): F[ServiceMessage] =
    (F.unique, serviceParams.zonedNow).mapN { case (id, ts) =>
      ServiceMessage(
        serviceParams = serviceParams,
        timestamp = ts,
        id = id.hashCode(),
        level = level,
        error = error,
        message = Encoder[S].apply(msg))
    }
}
