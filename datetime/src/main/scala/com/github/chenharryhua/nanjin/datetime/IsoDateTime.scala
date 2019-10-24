package com.github.chenharryhua.nanjin.datetime

import java.sql.{Date, Timestamp}
import java.time.{
  Instant,
  LocalDate,
  LocalDateTime,
  OffsetDateTime,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}

import monocle.Iso
import monocle.macros.Lenses

/**
  * for spark
  */
@Lenses final case class JavaOffsetDateTime private (instant: Instant, offset: Int) {
  val offsetDateTime: OffsetDateTime = instant.atOffset(ZoneOffset.ofTotalSeconds(offset))
}

object JavaOffsetDateTime {

  def apply(odt: OffsetDateTime): JavaOffsetDateTime =
    JavaOffsetDateTime(odt.toInstant, odt.getOffset.getTotalSeconds)

  implicit val isoJavaOffsetDateTime: Iso[OffsetDateTime, JavaOffsetDateTime] =
    Iso[OffsetDateTime, JavaOffsetDateTime](JavaOffsetDateTime(_))(_.offsetDateTime)
}

@Lenses final case class JavaZonedDateTime private (instant: Instant, zoneId: String) {
  val zonedDateTime: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of(zoneId))
}

object JavaZonedDateTime {

  def apply(zdt: ZonedDateTime): JavaZonedDateTime =
    JavaZonedDateTime(zdt.toInstant, zdt.getZone.getId)

  implicit val isoJavaZonedDateTime: Iso[ZonedDateTime, JavaZonedDateTime] =
    Iso[ZonedDateTime, JavaZonedDateTime](JavaZonedDateTime(_))(_.zonedDateTime)
}

private[datetime] trait IsoDateTimeInstance extends Serializable {

  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit def isoLocalDateTimeByZoneId(implicit zoneId: ZoneId): Iso[LocalDateTime, Timestamp] =
    Iso[LocalDateTime, Timestamp](a => isoInstant.get(a.atZone(zoneId).toInstant))(b =>
      LocalDateTime.ofInstant(isoInstant.reverseGet(b), zoneId))

  implicit val isoLocalDate: Iso[LocalDate, Date] =
    Iso[LocalDate, Date](Date.valueOf)(_.toLocalDate)
}
