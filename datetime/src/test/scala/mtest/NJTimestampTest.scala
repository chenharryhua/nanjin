package mtest

import java.time.{LocalDateTime, LocalTime, ZoneId}

import cats.syntax.eq._
import com.github.chenharryhua.nanjin.datetime._
import org.scalacheck.Prop.forAll
import org.scalacheck.Properties

class NJTimestampTest extends Properties("nj-timestamp properties") {
  import ArbitaryData._

  val timezone: ZoneId = utcTime
  property("day resolution") = forAll { (ts: NJTimestamp) =>
    ts.dayResolution(timezone) === ts.atZone(timezone).toLocalDate
  }
  property("hour resolution") = forAll { (ts: NJTimestamp) =>
    ts.hourResolution(timezone) === {
      val t = ts.atZone(timezone).toLocalDateTime
      LocalDateTime.of(t.toLocalDate, LocalTime.of(t.getHour, 0)).atZone(timezone)
    }
  }

  property("minute resolution") = forAll { (ts: NJTimestamp) =>
    ts.minuteResolution(timezone) === {
      val t = ts.atZone(timezone).toLocalDateTime
      LocalDateTime.of(t.toLocalDate, LocalTime.of(t.getHour, t.getMinute)).atZone(timezone)
    }
  }
}
