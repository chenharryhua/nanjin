package com.github.chenharryhua.nanjin.datetime

import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}

import cats.{Hash, Order, Show}
import io.chrisdavenport.cats.time.instances.all
import monocle.Iso

private[datetime] trait DateTimeInstances extends all {

  implicit final val timestampInstance: Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] =
    new Hash[Timestamp] with Order[Timestamp] with Show[Timestamp] {
      override def hash(x: Timestamp): Int                  = x.hashCode
      override def compare(x: Timestamp, y: Timestamp): Int = x.compareTo(y)
      override def show(x: Timestamp): String               = x.toString
    }

  implicit final val dateInstance: Hash[Date] with Order[Date] with Show[Date] =
    new Hash[Date] with Order[Date] with Show[Date] {
      override def hash(x: Date): Int             = x.hashCode
      override def compare(x: Date, y: Date): Int = x.compareTo(y)
      override def show(x: Date): String          = x.toString
    }
}

private[datetime] trait Isos {

  implicit val isoInstant: Iso[Instant, Timestamp] =
    Iso[Instant, Timestamp](Timestamp.from)(_.toInstant)

  implicit val isoLocalDate: Iso[LocalDate, Date] =
    Iso[LocalDate, Date](a => Date.valueOf(a))(b => b.toLocalDate)

}
