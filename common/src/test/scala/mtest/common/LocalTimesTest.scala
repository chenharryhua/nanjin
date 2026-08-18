package mtest.common

import com.github.chenharryhua.nanjin.common.chrono.localTimes
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalTime

class LocalTimesTest extends AnyFunSuite {

  test("1.midnight is 00:00") {
    assert(localTimes.midnight == LocalTime.of(0, 0, 0))
  }

  test("2.AM constants have correct hour values") {
    assert(localTimes.oneAM == LocalTime.of(1, 0, 0))
    assert(localTimes.twoAM == LocalTime.of(2, 0, 0))
    assert(localTimes.threeAM == LocalTime.of(3, 0, 0))
    assert(localTimes.fourAM == LocalTime.of(4, 0, 0))
    assert(localTimes.fiveAM == LocalTime.of(5, 0, 0))
    assert(localTimes.sixAM == LocalTime.of(6, 0, 0))
    assert(localTimes.sevenAM == LocalTime.of(7, 0, 0))
    assert(localTimes.eightAM == LocalTime.of(8, 0, 0))
    assert(localTimes.nineAM == LocalTime.of(9, 0, 0))
    assert(localTimes.tenAM == LocalTime.of(10, 0, 0))
    assert(localTimes.elevenAM == LocalTime.of(11, 0, 0))
  }

  test("3.noon is 12:00") {
    assert(localTimes.noon == LocalTime.of(12, 0, 0))
  }

  test("4.PM constants have correct hour values") {
    assert(localTimes.onePM == LocalTime.of(13, 0, 0))
    assert(localTimes.twoPM == LocalTime.of(14, 0, 0))
    assert(localTimes.threePM == LocalTime.of(15, 0, 0))
    assert(localTimes.fourPM == LocalTime.of(16, 0, 0))
    assert(localTimes.fivePM == LocalTime.of(17, 0, 0))
    assert(localTimes.sixPM == LocalTime.of(18, 0, 0))
    assert(localTimes.sevenPM == LocalTime.of(19, 0, 0))
    assert(localTimes.eightPM == LocalTime.of(20, 0, 0))
    assert(localTimes.ninePM == LocalTime.of(21, 0, 0))
    assert(localTimes.tenPM == LocalTime.of(22, 0, 0))
    assert(localTimes.elevenPM == LocalTime.of(23, 0, 0))
  }

  test("5.all constants have zero minutes and seconds") {
    val all = List(
      localTimes.midnight,
      localTimes.oneAM,
      localTimes.twoAM,
      localTimes.threeAM,
      localTimes.fourAM,
      localTimes.fiveAM,
      localTimes.sixAM,
      localTimes.sevenAM,
      localTimes.eightAM,
      localTimes.nineAM,
      localTimes.tenAM,
      localTimes.elevenAM,
      localTimes.noon,
      localTimes.onePM,
      localTimes.twoPM,
      localTimes.threePM,
      localTimes.fourPM,
      localTimes.fivePM,
      localTimes.sixPM,
      localTimes.sevenPM,
      localTimes.eightPM,
      localTimes.ninePM,
      localTimes.tenPM,
      localTimes.elevenPM
    )
    assert(all.size == 24)
    assert(all.forall(t => t.getMinute == 0 && t.getSecond == 0 && t.getNano == 0))
  }

  test("6.constants are in ascending order from midnight to elevenPM") {
    val all = List(
      localTimes.midnight,
      localTimes.oneAM,
      localTimes.twoAM,
      localTimes.threeAM,
      localTimes.fourAM,
      localTimes.fiveAM,
      localTimes.sixAM,
      localTimes.sevenAM,
      localTimes.eightAM,
      localTimes.nineAM,
      localTimes.tenAM,
      localTimes.elevenAM,
      localTimes.noon,
      localTimes.onePM,
      localTimes.twoPM,
      localTimes.threePM,
      localTimes.fourPM,
      localTimes.fivePM,
      localTimes.sixPM,
      localTimes.sevenPM,
      localTimes.eightPM,
      localTimes.ninePM,
      localTimes.tenPM,
      localTimes.elevenPM
    )
    assert(all.sliding(2).forall { case List(a, b) => a.isBefore(b); case _ => true })
  }
}
