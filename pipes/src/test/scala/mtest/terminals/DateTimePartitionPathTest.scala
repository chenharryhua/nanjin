package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.DateTimePartitionPath
import munit.FunSuite

import java.time.{LocalDate, LocalDateTime}

class DateTimePartitionPathTest extends FunSuite {

  test("1.ymd, ymdh, and ymdhm use fixed-width partition fields") {
    val date = LocalDate.of(2024, 2, 29)
    val date_time = LocalDateTime.of(2024, 2, 29, 3, 4, 5)

    assertEquals(DateTimePartitionPath.ymd(date), "Year=2024/Month=02/Day=29")
    assertEquals(DateTimePartitionPath.ymdh(date_time), "Year=2024/Month=02/Day=29/Hour=03")
    assertEquals(DateTimePartitionPath.ymdhm(date_time), "Year=2024/Month=02/Day=29/Hour=03/Minute=04")
  }

  test("2.parsers accept lower and upper field bounds") {
    assertEquals(DateTimePartitionPath.year("Year=0000"), Some(0))
    assertEquals(DateTimePartitionPath.year("Year=9999"), Some(9999))
    assertEquals(DateTimePartitionPath.month("Month=01"), Some(1))
    assertEquals(DateTimePartitionPath.month("Month=12"), Some(12))
    assertEquals(DateTimePartitionPath.day("Day=01"), Some(1))
    assertEquals(DateTimePartitionPath.day("Day=31"), Some(31))
    assertEquals(DateTimePartitionPath.hour("Hour=00"), Some(0))
    assertEquals(DateTimePartitionPath.hour("Hour=23"), Some(23))
    assertEquals(DateTimePartitionPath.minute("Minute=00"), Some(0))
    assertEquals(DateTimePartitionPath.minute("Minute=59"), Some(59))
  }

  test("3.parsers reject out-of-range values") {
    val invalid = List(
      DateTimePartitionPath.month("Month=00"),
      DateTimePartitionPath.month("Month=13"),
      DateTimePartitionPath.day("Day=00"),
      DateTimePartitionPath.day("Day=32"),
      DateTimePartitionPath.hour("Hour=24"),
      DateTimePartitionPath.minute("Minute=60")
    )

    assert(invalid.forall(_.isEmpty))
  }

  test("4.parsers require exact widths, numeric input, and no trailing content") {
    val invalid = List(
      DateTimePartitionPath.year("Year=123"),
      DateTimePartitionPath.year("Year=10000"),
      DateTimePartitionPath.month("Month=1"),
      DateTimePartitionPath.day("Day=abc"),
      DateTimePartitionPath.hour("Hour=01-extra"),
      DateTimePartitionPath.minute("Minute= 01")
    )

    assert(invalid.forall(_.isEmpty))
  }

  test("5.field names are case-insensitive but otherwise exact") {
    assertEquals(DateTimePartitionPath.year("year=2026"), Some(2026))
    assertEquals(DateTimePartitionPath.month("mOnTh=03"), Some(3))
    assertEquals(DateTimePartitionPath.day("DAY=04"), Some(4))
    assertEquals(DateTimePartitionPath.hour("hour=05"), Some(5))
    assertEquals(DateTimePartitionPath.minute("minute=06"), Some(6))
    assertEquals(DateTimePartitionPath.year("Years=2026"), None)
  }
}
