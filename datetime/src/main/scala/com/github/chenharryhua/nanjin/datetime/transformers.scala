package com.github.chenharryhua.nanjin.datetime

import java.time._

import io.scalaland.chimney.Transformer

object transformers {

  implicit val zonedDateTimeTransform: Transformer[ZonedDateTime, Instant] =
    (src: ZonedDateTime) => src.toInstant

  implicit def zonedDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Instant, ZonedDateTime] =
    (src: Instant) => ZonedDateTime.ofInstant(src, zoneId)

  implicit val offsetDateTimeTransform: Transformer[OffsetDateTime, Instant] =
    (src: OffsetDateTime) => Instant.from(src.toInstant)

  implicit def offsetDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Instant, OffsetDateTime] =
    (src: Instant) => OffsetDateTime.ofInstant(src, zoneId)

  implicit def localDateTimeTransform(implicit
    zoneId: ZoneId): Transformer[LocalDateTime, Instant] =
    (src: LocalDateTime) => src.atZone(zoneId).toInstant

  implicit def localDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Instant, LocalDateTime] =
    (src: Instant) => src.atZone(zoneId).toLocalDateTime
}
