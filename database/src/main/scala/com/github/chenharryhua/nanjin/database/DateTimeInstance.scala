package com.github.chenharryhua.nanjin.database
import java.sql.{Date, Timestamp}
import java.time._

import doobie.util.Meta
import monocle.Iso
import monocle.macros.Lenses

@Lenses final case class JavaOffsetDateTime private (dateTime: Instant, offset: Int) {
  val offsetDateTime: OffsetDateTime = dateTime.atOffset(ZoneOffset.ofTotalSeconds(offset))
}

object JavaOffsetDateTime {

  def apply(odt: OffsetDateTime): JavaOffsetDateTime =
    JavaOffsetDateTime(odt.toInstant, odt.getOffset.getTotalSeconds)

  implicit val isoJavaOffsetDateTime: Iso[OffsetDateTime, JavaOffsetDateTime] =
    Iso[OffsetDateTime, JavaOffsetDateTime](JavaOffsetDateTime(_))(_.offsetDateTime)
}

@Lenses final case class JavaZonedDateTime private (dateTime: Instant, zoneId: String) {
  val zonedDateTime: ZonedDateTime = ZonedDateTime.ofInstant(dateTime, ZoneId.of(zoneId))
}

object JavaZonedDateTime {

  def apply(zdt: ZonedDateTime): JavaZonedDateTime =
    JavaZonedDateTime(zdt.toInstant, zdt.getZone.getId)

  implicit val isoJavaZonedDateTime: Iso[ZonedDateTime, JavaZonedDateTime] =
    Iso[ZonedDateTime, JavaZonedDateTime](JavaZonedDateTime(_))(_.zonedDateTime)
}

private trait IsoDateTimeInstance extends Serializable {

  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit def isoLocalDateTimeByZoneId(implicit zoneId: ZoneId): Iso[LocalDateTime, Timestamp] =
    Iso[LocalDateTime, Timestamp](a => isoInstant.get(a.atZone(zoneId).toInstant))(b =>
      LocalDateTime.ofInstant(isoInstant.reverseGet(b), zoneId))

  implicit val isoLocalDate: Iso[LocalDate, Date] =
    Iso[LocalDate, Date](Date.valueOf)(_.toLocalDate)

}

private[database] trait MetaDateTimeInstance extends IsoDateTimeInstance {

  implicit def metaDoobieTimestamp[A](implicit iso: Iso[A, Timestamp]): Meta[A] =
    Meta[Timestamp].imap(iso.reverseGet)(iso.get)

  implicit def metaDoobieDate[A](implicit iso: Iso[A, Date]): Meta[A] =
    Meta[Date].imap(iso.reverseGet)(iso.get)
}
