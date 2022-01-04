package com.github.chenharryhua.nanjin.guard

import cats.effect.kernel.Temporal
import cats.syntax.functor.*

import java.time.{ZoneId, ZonedDateTime}

package object event {
  def realZonedDateTime[F[_]](zoneId: ZoneId)(implicit F: Temporal[F]): F[ZonedDateTime] =
    F.realTimeInstant.map(_.atZone(zoneId))
}
