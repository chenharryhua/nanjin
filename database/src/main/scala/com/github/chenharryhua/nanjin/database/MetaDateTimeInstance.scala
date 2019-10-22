package com.github.chenharryhua.nanjin.database
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import doobie.util.Meta
import monocle.Iso

private trait IsoDateTimeInstance {

  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit def isoLocalDateTime(implicit zoneId: ZoneId): Iso[LocalDateTime, Timestamp] =
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
