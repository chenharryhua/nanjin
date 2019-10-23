package mtest

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.database._
import com.github.chenharryhua.nanjin.spark._
import frameless.{Injection, TypedEncoder}
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties
import cats.implicits._

class TimeInjectionProps extends Properties("date time") {
  implicit val zoneId = ZoneId.systemDefault()
  val date            = TypedEncoder[Date]
  val timestamp       = TypedEncoder[Timestamp]
  val lcoaldate       = TypedEncoder[LocalDate]
  val localdatetime   = TypedEncoder[LocalDateTime]
  val instant         = TypedEncoder[Instant]
  val zoneddatetime   = TypedEncoder[ZonedDateTime]
  val offsetdatetime  = TypedEncoder[OffsetDateTime]

  property("timezone has no effect on epoch-second") = forAll { (ins: Instant) =>
    val tz1: ZoneId = ZoneId.of("Australia/Sydney")
    val tz2: ZoneId = ZoneId.of("America/Phoenix")
    ZonedDateTime.ofInstant(ins, tz1).toEpochSecond ==
      ZonedDateTime.ofInstant(ins, tz2).toEpochSecond
  }
  property("invertable LocalDateTime") = forAll { (ins: LocalDateTime) =>
    val in = implicitly[Injection[LocalDateTime, Timestamp]]
    in.invert(in.apply(ins)) == ins
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
    val in = implicitly[Injection[LocalDate, Date]]
    in.invert(in.apply(ins)) == ins
  }
  property("invertable Instant") = forAll { (ins: Instant) =>
    val in = implicitly[Injection[Instant, Timestamp]]
    in.invert(in.apply(ins)) == ins
  }
}
