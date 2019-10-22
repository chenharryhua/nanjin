package mtest

import java.sql.Date
import java.time.{Instant, LocalDate, LocalDateTime, ZoneId}

import com.github.chenharryhua.nanjin.spark._
import com.github.chenharryhua.nanjin.database._

import frameless.TypedEncoder
import org.scalacheck.Properties
import java.sql.Timestamp

class TimeInjectionProps extends Properties("Injection") {
  implicit val zoneId = ZoneId.systemDefault()
  val date            = TypedEncoder[Date]
  val timestamp       = TypedEncoder[Timestamp]
  val lcoaldate       = TypedEncoder[LocalDate]
  val localdatetime   = TypedEncoder[LocalDateTime]
  val instant         = TypedEncoder[Instant]
}
