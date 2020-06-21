package com.github.chenharryhua.nanjin.spark

import java.sql.Timestamp
import java.time._

import io.scalaland.chimney.Transformer
import monocle.Iso

package object datetime {

  implicit val isoLocalTime: Iso[LocalTime, JavaLocalTime] =
    Iso[LocalTime, JavaLocalTime](JavaLocalTime(_))(_.localTime)

  implicit def isoLocalDateTime: Iso[LocalDateTime, JavaLocalDateTime] =
    Iso[LocalDateTime, JavaLocalDateTime](JavaLocalDateTime(_))(_.localDateTime)

  implicit val isoOffsetDateTime: Iso[OffsetDateTime, JavaOffsetDateTime] =
    Iso[OffsetDateTime, JavaOffsetDateTime](JavaOffsetDateTime(_))(_.offsetDateTime)

  implicit val isoZonedDateTime: Iso[ZonedDateTime, JavaZonedDateTime] =
    Iso[ZonedDateTime, JavaZonedDateTime](JavaZonedDateTime(_))(_.zonedDateTime)

  // can not go back
  implicit val zonedDateTimeTransform: Transformer[ZonedDateTime, Timestamp] =
    (src: ZonedDateTime) => Timestamp.from(src.toInstant)

  implicit val offsetDateTimeTransform: Transformer[OffsetDateTime, Timestamp] =
    (src: OffsetDateTime) => Timestamp.from(src.toInstant)
}
