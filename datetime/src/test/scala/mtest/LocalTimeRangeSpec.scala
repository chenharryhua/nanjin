package mtest

import com.github.chenharryhua.nanjin.datetime.LocalTimeRange
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.*
import scala.concurrent.duration.DurationInt

class LocalTimeRangeSpec extends AnyFlatSpec with Matchers {

  "LocalTimeRange.inBetween(LocalTime)" should "return false for negative durations" in {
    val ltr = LocalTimeRange(LocalTime.of(10, 0), Duration.ofHours(-1))
    ltr.inBetween(LocalTime.of(10, 0)) shouldBe false
    ltr.inBetween(LocalTime.of(9, 0)) shouldBe false
  }

  it should "return true for duration >= 24 hours" in {
    val ltr = LocalTimeRange(LocalTime.of(0, 0), Duration.ofHours(24))
    ltr.inBetween(LocalTime.of(0, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(12, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(23, 59)) shouldBe true
  }

  it should "handle normal non-cross-midnight durations correctly" in {
    val ltr = LocalTimeRange(LocalTime.of(10, 0), Duration.ofHours(5)) // 10:00 -> 15:00
    ltr.inBetween(LocalTime.of(9, 0)) shouldBe false
    ltr.inBetween(LocalTime.of(10, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(12, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(15, 0)) shouldBe false
    ltr.inBetween(LocalTime.of(16, 0)) shouldBe false
  }

  it should "handle cross-midnight durations correctly" in {
    val ltr = LocalTimeRange(LocalTime.of(23, 0), Duration.ofHours(2)) // 23:00 -> 01:00
    ltr.inBetween(LocalTime.of(22, 59)) shouldBe false
    ltr.inBetween(LocalTime.of(23, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(23, 30)) shouldBe true
    ltr.inBetween(LocalTime.of(0, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(0, 59)) shouldBe true
    ltr.inBetween(LocalTime.of(1, 0)) shouldBe false
    ltr.inBetween(LocalTime.of(2, 0)) shouldBe false
  }

  it should "handle exact zero duration" in {
    val ltr = LocalTimeRange(LocalTime.of(12, 0), Duration.ZERO)
    ltr.inBetween(LocalTime.of(12, 0)) shouldBe false
    ltr.inBetween(LocalTime.of(12, 1)) shouldBe false
  }

  "LocalTimeRange.inBetween(ZonedDateTime)" should "delegate to LocalTime correctly" in {
    val ltr = LocalTimeRange(LocalTime.of(22, 0), Duration.ofHours(4)) // 22:00 -> 02:00
    val zdt1 = ZonedDateTime.of(LocalDate.of(2026, 2, 1), LocalTime.of(23, 0), ZoneId.of("UTC"))
    val zdt2 = ZonedDateTime.of(LocalDate.of(2026, 2, 2), LocalTime.of(1, 30), ZoneId.of("UTC"))
    val zdt3 = ZonedDateTime.of(LocalDate.of(2026, 2, 2), LocalTime.of(2, 0), ZoneId.of("UTC"))

    ltr.inBetween(zdt1) shouldBe true
    ltr.inBetween(zdt2) shouldBe true
    ltr.inBetween(zdt3) shouldBe false
  }

  "LocalTimeRange companion apply" should "convert FiniteDuration to Duration" in {
    val ltr = LocalTimeRange(LocalTime.of(10, 0), 2.hours)
    ltr.inBetween(LocalTime.of(11, 0)) shouldBe true
    ltr.inBetween(LocalTime.of(12, 0)) shouldBe false
  }
}
