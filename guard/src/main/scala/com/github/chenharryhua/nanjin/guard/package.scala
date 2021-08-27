package com.github.chenharryhua.nanjin

import cats.effect.kernel.Temporal
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams

import java.time.ZonedDateTime

package object guard {
  private[guard] def realZonedDateTime[F[_]](serviceParams: ServiceParams)(implicit F: Temporal[F]): F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  private[guard] def passThroughMRName(desc: String): String = s"06.[$desc].number"
  private[guard] def actionFailMRName(name: String): String  = s"07.[$name].`fail`"

  // number retrys and succed have 6 chars so that the number after is aligned
  private[guard] def actionStartMRName(name: String): String = s"07.[$name].number"
  private[guard] def actionRetryMRName(name: String): String = s"07.[$name].retrys"
  private[guard] def actionSuccMRName(name: String): String  = s"07.[$name].succed"
}
