package com.github.chenharryhua.nanjin.datetime

import java.sql.Timestamp
import java.time._

import io.scalaland.chimney.Transformer
import monocle.Iso

object transformers extends LowPriorityReverse {

  implicit val zonedDateTimeTransform: Transformer[ZonedDateTime, Instant] =
    (src: ZonedDateTime) => src.toInstant

  implicit val offsetDateTimeTransform: Transformer[OffsetDateTime, Instant] =
    (src: OffsetDateTime) => src.toInstant

  implicit def localDateTimeTransform(implicit
    zoneId: ZoneId): Transformer[LocalDateTime, Instant] =
    (src: LocalDateTime) => src.atZone(zoneId).toInstant

  // generic
  implicit def chimneyTransform[A, B](implicit iso: Iso[A, B]): Transformer[A, B] =
    (src: A) => iso.get(src)

  implicit def instantTransform[A](implicit
    trans: Transformer[A, Instant]): Transformer[A, Timestamp] =
    (src: A) => isoInstant.get(trans.transform(src))
}

private[datetime] trait LowPriorityReverse {

  implicit def localDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Instant, LocalDateTime] =
    (src: Instant) => src.atZone(zoneId).toLocalDateTime

  implicit def offsetDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Instant, OffsetDateTime] =
    (src: Instant) => OffsetDateTime.ofInstant(src, zoneId)

  implicit def zonedDateTimeTransformReverse(implicit
    zoneId: ZoneId): Transformer[Instant, ZonedDateTime] =
    (src: Instant) => ZonedDateTime.ofInstant(src, zoneId)

  //generic
  implicit def chimneyTransformReverse[A, B](implicit iso: Iso[A, B]): Transformer[B, A] =
    (src: B) => iso.reverseGet(src)

  implicit def instantTransformReverse[A](implicit
    trans: Transformer[Instant, A]): Transformer[Timestamp, A] =
    (src: Timestamp) => trans.transform(isoInstant.reverseGet(src))

}
