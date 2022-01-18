package mtest

import cats.syntax.eq.*
import com.github.chenharryhua.nanjin.datetime.*
import com.github.chenharryhua.nanjin.datetime.instances.*
import org.scalacheck.Prop.{forAll, propBoolean}
import org.scalacheck.Properties

import java.time.{LocalDateTime, LocalTime, ZoneId}

class NJTimestampTest extends Properties("nj-timestamp properties") {
  import ArbitaryData.*

  val timezone: ZoneId = utcTime

  property("+ -") =
    forAll((ts: NJTimestamp, long: Long) => (long < 10000000L && long >= 0) ==> (ts.minus(long).plus(long) === ts))

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
