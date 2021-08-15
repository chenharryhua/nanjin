package com.github.chenharryhua.nanjin

import cats.effect.kernel.Temporal
import cats.syntax.functor.*
import com.github.chenharryhua.nanjin.guard.config.ServiceParams

import java.time.ZonedDateTime

package object guard {
  private[guard] def realZonedDateTime[F[_]](serviceParams: ServiceParams)(implicit F: Temporal[F]): F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(serviceParams.taskParams.zoneId))

  private[guard] def actionFailMRName(name: String): String  = s"07.`fail`.[$name]"
  private[guard] def actionStartMRName(name: String): String = s"08.count.[$name]"
  private[guard] def actionRetryMRName(name: String): String = s"08.retry.[$name]"
  private[guard] def actionSuccMRName(name: String): String  = s"08.succ.[$name]"
}
