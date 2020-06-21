package com.github.chenharryhua.nanjin.datetime

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import monocle.Iso

private[datetime] trait Isos {

  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit val isoLocalDate: Iso[LocalDate, Date] =
    Iso[LocalDate, Date](a => Date.valueOf(a))(b => b.toLocalDate)

}
