package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._

import io.scalaland.chimney.Transformer

object transformers {

  implicit val zonedDateTimeTransform: Transformer[ZonedDateTime, Timestamp] =
    (src: ZonedDateTime) => Timestamp.from(src.toInstant)

  implicit def zonedDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Timestamp, ZonedDateTime] =
    (src: Timestamp) => ZonedDateTime.ofInstant(src.toInstant, zoneId)

  implicit val offsetDateTimeTransform: Transformer[OffsetDateTime, Timestamp] =
    (src: OffsetDateTime) => Timestamp.from(Instant.from(src.toInstant))

  implicit def offsetDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Timestamp, OffsetDateTime] =
    (src: Timestamp) => OffsetDateTime.ofInstant(src.toInstant, zoneId)

  implicit def localDateTimeTransform(implicit
    zoneId: ZoneId): Transformer[LocalDateTime, Timestamp] =
    (src: LocalDateTime) => Timestamp.from(src.atZone(zoneId).toInstant)

  implicit def localDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Timestamp, LocalDateTime] =
    (src: Timestamp) => src.toInstant.atZone(zoneId).toLocalDateTime
}
