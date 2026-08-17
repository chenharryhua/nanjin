package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.zones
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId

class ZonesTest extends AnyFunSuite {

  test("1.utcTime is Etc/UTC") {
    assert(zones.utcTime == ZoneId.of("Etc/UTC"))
    assert(zones.utcTime.getId == "Etc/UTC")
  }

  test("2.Australian zones are valid") {
    assert(zones.darwinTime == ZoneId.of("Australia/Darwin"))
    assert(zones.sydneyTime == ZoneId.of("Australia/Sydney"))
  }

  test("3.Asian zones are valid") {
    assert(zones.beijingTime == ZoneId.of("Asia/Shanghai"))
    assert(zones.singaporeTime == ZoneId.of("Asia/Singapore"))
    assert(zones.mumbaiTime == ZoneId.of("Asia/Kolkata"))
  }

  test("4.American zones are valid") {
    assert(zones.newyorkTime == ZoneId.of("America/New_York"))
    assert(zones.saltaTime == ZoneId.of("America/Argentina/Salta"))
  }

  test("5.European zones are valid") {
    assert(zones.londonTime == ZoneId.of("Europe/London"))
    assert(zones.berlinTime == ZoneId.of("Europe/Berlin"))
  }

  test("6.African zones are valid") {
    assert(zones.cairoTime == ZoneId.of("Africa/Cairo"))
  }

  test("7.all zone IDs are recognized by java.time") {
    val all = List(
      zones.utcTime, zones.darwinTime, zones.sydneyTime, zones.beijingTime,
      zones.singaporeTime, zones.mumbaiTime, zones.newyorkTime, zones.londonTime,
      zones.berlinTime, zones.cairoTime, zones.saltaTime
    )
    val availableZones = ZoneId.getAvailableZoneIds
    assert(all.forall(z => availableZones.contains(z.getId)))
  }
}
