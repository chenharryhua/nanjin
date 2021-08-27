package com.github.chenharryhua.nanjin

import cats.effect.kernel.Temporal
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams

import java.time.ZonedDateTime

package object guard {
  private[guard] def realZonedDateTime[F[_]](serviceParams: ServiceParams)(implicit F: Temporal[F]): F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  private[guard] def passThroughMRName(desc: String): String = s"06.[$desc].count"
  private[guard] def actionFailMRName(name: String): String  = s"07.[$name].`fail`"

  // alignment
  private[guard] def actionStartMRName(name: String): String = s"07.[$name].count"
  private[guard] def actionRetryMRName(name: String): String = s"07.[$name].retry"
  private[guard] def actionSuccMRName(name: String): String  = s"07.[$name].succd"
}
