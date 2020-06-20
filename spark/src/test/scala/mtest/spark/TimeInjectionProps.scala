package mtest.spark

import java.sql.{Date, Timestamp}
import java.time._

import com.fortysevendeg.scalacheck.datetime.jdk8.ArbitraryJdk8._
import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import com.github.chenharryhua.nanjin.spark.injection._
import com.github.chenharryhua.nanjin.spark.datetime._
import frameless.TypedEncoder
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

class TimeInjectionProps extends Properties("date time") {
  // must compile
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
}
