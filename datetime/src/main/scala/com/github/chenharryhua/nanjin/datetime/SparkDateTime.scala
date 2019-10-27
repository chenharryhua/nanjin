package com.github.chenharryhua.nanjin.datetime

import java.time._

import cats.implicits._
import cats.{Hash, Order, Show}
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

  implicit val javaOffsetDateTimeInstance
    : Hash[JavaOffsetDateTime] with Order[JavaOffsetDateTime] with Show[JavaOffsetDateTime] =
    new Hash[JavaOffsetDateTime] with Order[JavaOffsetDateTime] with Show[JavaOffsetDateTime] {
      override def hash(x: JavaOffsetDateTime): Int = x.hashCode

      override def compare(x: JavaOffsetDateTime, y: JavaOffsetDateTime): Int =
        x.offsetDateTime.compareTo(y.offsetDateTime)
      override def show(x: JavaOffsetDateTime): String = x.offsetDateTime.show
    }
}

@Lenses final case class JavaZonedDateTime private (instant: Instant, zoneId: String) {
  val zonedDateTime: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of(zoneId))
}

object JavaZonedDateTime {

  def apply(zdt: ZonedDateTime): JavaZonedDateTime =
    JavaZonedDateTime(zdt.toInstant, zdt.getZone.getId)

  implicit val isoJavaZonedDateTime: Iso[ZonedDateTime, JavaZonedDateTime] =
    Iso[ZonedDateTime, JavaZonedDateTime](JavaZonedDateTime(_))(_.zonedDateTime)

  implicit val javaZonedDateTimeInstance
    : Hash[JavaZonedDateTime] with Order[JavaZonedDateTime] with Show[JavaZonedDateTime] =
    new Hash[JavaZonedDateTime] with Order[JavaZonedDateTime] with Show[JavaZonedDateTime] {
      override def hash(x: JavaZonedDateTime): Int = x.hashCode

      override def compare(x: JavaZonedDateTime, y: JavaZonedDateTime): Int =
        x.zonedDateTime.compareTo(y.zonedDateTime)
      override def show(x: JavaZonedDateTime): String = x.zonedDateTime.show
    }
}
