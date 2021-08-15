package com.github.chenharryhua.nanjin

import cats.effect.kernel.Temporal
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams

import java.time.ZonedDateTime

package object guard {
  private[guard] def realZonedDateTime[F[_]](serviceParams: ServiceParams)(implicit F: Temporal[F]): F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  private[guard] val actionStartEventCounterName = "05.action.count"
}
