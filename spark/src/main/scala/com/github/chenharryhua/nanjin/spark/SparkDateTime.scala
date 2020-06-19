package com.github.chenharryhua.nanjin.spark

import java.sql.Timestamp
import java.time._

import cats.implicits._
import cats.{Hash, Order, Show}
import com.github.chenharryhua.nanjin.datetime._

// spark doesn't spart java.time, yet
final case class JavaOffsetDateTime private (instant: Instant, offset: Int) {
  val offsetDateTime: OffsetDateTime = instant.atOffset(ZoneOffset.ofTotalSeconds(offset))
  val timestamp: Timestamp           = Timestamp.from(instant)
}

object JavaOffsetDateTime {

  def apply(odt: OffsetDateTime): JavaOffsetDateTime =
    JavaOffsetDateTime(odt.toInstant, odt.getOffset.getTotalSeconds)

  implicit val javaOffsetDateTimeInstance
    : Hash[JavaOffsetDateTime] with Order[JavaOffsetDateTime] with Show[JavaOffsetDateTime] =
    new Hash[JavaOffsetDateTime] with Order[JavaOffsetDateTime] with Show[JavaOffsetDateTime] {
      override def hash(x: JavaOffsetDateTime): Int = x.hashCode

      override def compare(x: JavaOffsetDateTime, y: JavaOffsetDateTime): Int =
        x.offsetDateTime.compareTo(y.offsetDateTime)
      override def show(x: JavaOffsetDateTime): String = x.offsetDateTime.show
    }
}

final case class JavaZonedDateTime private (instant: Instant, zoneId: String) {
  val zonedDateTime: ZonedDateTime = ZonedDateTime.ofInstant(instant, ZoneId.of(zoneId))
  val timestamp: Timestamp         = Timestamp.from(instant)
}

object JavaZonedDateTime {

  def apply(zdt: ZonedDateTime): JavaZonedDateTime =
    JavaZonedDateTime(zdt.toInstant, zdt.getZone.getId)

  implicit val javaZonedDateTimeInstance
    : Hash[JavaZonedDateTime] with Order[JavaZonedDateTime] with Show[JavaZonedDateTime] =
    new Hash[JavaZonedDateTime] with Order[JavaZonedDateTime] with Show[JavaZonedDateTime] {
      override def hash(x: JavaZonedDateTime): Int = x.hashCode

      override def compare(x: JavaZonedDateTime, y: JavaZonedDateTime): Int =
        x.zonedDateTime.compareTo(y.zonedDateTime)
      override def show(x: JavaZonedDateTime): String = x.zonedDateTime.show
    }
}

final case class JavaLocalDate private (year: Int, month: Int, day: Int) {
  val localDate: LocalDate = LocalDate.of(year, month, day)
}

object JavaLocalDate {

  def apply(ld: LocalDate): JavaLocalDate =
    JavaLocalDate(ld.getYear, ld.getMonthValue, ld.getDayOfMonth)

  implicit val javaLocalDateInstance
    : Hash[JavaLocalDate] with Order[JavaLocalDate] with Show[JavaLocalDate] =
    new Hash[JavaLocalDate] with Order[JavaLocalDate] with Show[JavaLocalDate] {
      override def hash(x: JavaLocalDate): Int = x.hashCode

      override def compare(x: JavaLocalDate, y: JavaLocalDate): Int =
        x.localDate.compareTo(y.localDate)

      override def show(x: JavaLocalDate): String = x.localDate.show
    }
}

final case class JavaLocalTime private (hour: Int, minute: Int, second: Int, nanoOfSecond: Int) {
  val localTime: LocalTime = LocalTime.of(hour, minute, second, nanoOfSecond)
}

object JavaLocalTime {

  def apply(lt: LocalTime): JavaLocalTime =
    JavaLocalTime(lt.getHour, lt.getMinute, lt.getSecond, lt.getNano)

  implicit val javaLocalTimeInstance
    : Hash[JavaLocalTime] with Order[JavaLocalTime] with Show[JavaLocalTime] =
    new Hash[JavaLocalTime] with Order[JavaLocalTime] with Show[JavaLocalTime] {
      override def hash(x: JavaLocalTime): Int = x.hashCode

      override def compare(x: JavaLocalTime, y: JavaLocalTime): Int =
        x.localTime.compareTo(y.localTime)

      override def show(x: JavaLocalTime): String = x.localTime.show
    }
}

final case class JavaLocalDateTime private (date: LocalDate, time: LocalTime) {
  val localDateTime: LocalDateTime = LocalDateTime.of(date, time)
}

object JavaLocalDateTime {

  def apply(ldt: LocalDateTime): JavaLocalDateTime =
    JavaLocalDateTime(ldt.toLocalDate, ldt.toLocalTime)

  implicit val javaLocalDateTimeInstance
    : Hash[JavaLocalDateTime] with Order[JavaLocalDateTime] with Show[JavaLocalDateTime] =
    new Hash[JavaLocalDateTime] with Order[JavaLocalDateTime] with Show[JavaLocalDateTime] {
      override def hash(x: JavaLocalDateTime): Int = x.hashCode

      override def compare(x: JavaLocalDateTime, y: JavaLocalDateTime): Int =
        x.localDateTime.compareTo(y.localDateTime)

      override def show(x: JavaLocalDateTime): String = x.localDateTime.show
    }
}
