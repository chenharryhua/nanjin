package mtest.spark

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.spark.injection._
import frameless.{Injection, TypedEncoder}
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

class TimeInjectionProps extends Properties("date time") {
  val date           = TypedEncoder[Date]
  val timestamp      = TypedEncoder[Timestamp]
  val localdate      = TypedEncoder[LocalDate]
  val localtime      = TypedEncoder[LocalTime]
  val localdatetime  = TypedEncoder[LocalDateTime]
  val instant        = TypedEncoder[Instant]
  val zoneddatetime  = TypedEncoder[ZonedDateTime]
  val offsetdatetime = TypedEncoder[OffsetDateTime]
  val njtimestamp    = TypedEncoder[NJTimestamp]

  property("timezone has no effect on epoch-second") = forAll { (ins: Instant) =>
    val tz1: ZoneId = ZoneId.of("Australia/Sydney")
    val tz2: ZoneId = ZoneId.of("America/Phoenix")
    ZonedDateTime.ofInstant(ins, tz1).toEpochSecond ==
      ZonedDateTime.ofInstant(ins, tz2).toEpochSecond
  }
  property("invertable LocalDateTime") = forAll { (ins: LocalDateTime) =>
    val in = implicitly[Injection[LocalDateTime, JavaLocalDateTime]]
    if (ins.getYear > 1900 && ins.getYear < 8099) in.invert(in.apply(ins)) == ins else true
  }
  property("invertable ZonedDateTime") = forAll { (ins: ZonedDateTime) =>
    val in = implicitly[Injection[ZonedDateTime, JavaZonedDateTime]]
    in.invert(in.apply(ins)) == ins
  }
  property("invertable OffsetDateTime") = forAll { (ins: ZonedDateTime) =>
    val in = implicitly[Injection[OffsetDateTime, JavaOffsetDateTime]]
    in.invert(in.apply(ins.toOffsetDateTime)) == ins.toOffsetDateTime
  }
  property("invertable LocalDate") = forAll { (ins: LocalDate) =>
    val in = implicitly[Injection[LocalDate, JavaLocalDate]]
    if (ins.getYear > 1900 && ins.getYear < 8099) in.invert(in.apply(ins)) == ins else true
  }
  property("invertable Instant") = forAll { (ins: Instant) =>
    val in = implicitly[Injection[Instant, Timestamp]]
    in.invert(in.apply(ins)) == ins
  }
}
