package mtest

import java.time.{LocalDate, ZoneId}

import com.github.chenharryhua.nanjin.datetime.NJTimestamp
import org.scalatest.funsuite.AnyFunSuite

class NJTimestampTest extends AnyFunSuite {
  val date   = LocalDate.of(2012, 1, 26)
  val zoneId = ZoneId.of("Australia/Melbourne")
  val nj     = NJTimestamp(date, zoneId)
  test("year moth day") {
    assert(nj.yearStr(zoneId) === "2012")
    assert(nj.monthStr(zoneId) === "01")
    assert(nj.dayStr(zoneId) === "26")
  }
}
