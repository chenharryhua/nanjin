package mtest

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.database._
import com.github.chenharryhua.nanjin.spark._
import frameless.TypedEncoder
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

class TimeInjectionProps extends Properties("date time") {
  implicit val zoneId = ZoneId.systemDefault()
  val date            = TypedEncoder[Date]
  val timestamp       = TypedEncoder[Timestamp]
  val lcoaldate       = TypedEncoder[LocalDate]
  val localdatetime   = TypedEncoder[LocalDateTime]
  val instant         = TypedEncoder[Instant]

  property("timezone has no effect on epoch-second") = forAll { (ins: Instant) =>
    val tz1: ZoneId = ZoneId.of("Australia/Sydney")
    val tz2: ZoneId = ZoneId.of("America/Phoenix")
    ZonedDateTime.ofInstant(ins, tz1).toEpochSecond ==
      ZonedDateTime.ofInstant(ins, tz2).toEpochSecond
  }
}
