package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.partitionPath
import munit.FunSuite

import java.time.{LocalDate, LocalDateTime}

class PartitionPathTest extends FunSuite {

  test("1.ymd, ymdh, and ymdhm use fixed-width partition fields") {
    val date = LocalDate.of(2024, 2, 29)
    val date_time = LocalDateTime.of(2024, 2, 29, 3, 4, 5)

    assertEquals(partitionPath.ymd(date), "Year=2024/Month=02/Day=29")
    assertEquals(partitionPath.ymdh(date_time), "Year=2024/Month=02/Day=29/Hour=03")
    assertEquals(partitionPath.ymdhm(date_time), "Year=2024/Month=02/Day=29/Hour=03/Minute=04")
  }

  test("2.parsers accept lower and upper field bounds") {
    assertEquals(partitionPath.year("Year=0000"), Some(0))
    assertEquals(partitionPath.year("Year=9999"), Some(9999))
    assertEquals(partitionPath.month("Month=01"), Some(1))
    assertEquals(partitionPath.month("Month=12"), Some(12))
    assertEquals(partitionPath.day("Day=01"), Some(1))
    assertEquals(partitionPath.day("Day=31"), Some(31))
    assertEquals(partitionPath.hour("Hour=00"), Some(0))
    assertEquals(partitionPath.hour("Hour=23"), Some(23))
    assertEquals(partitionPath.minute("Minute=00"), Some(0))
    assertEquals(partitionPath.minute("Minute=59"), Some(59))
  }

  test("3.parsers reject out-of-range values") {
    val invalid = List(
      partitionPath.month("Month=00"),
      partitionPath.month("Month=13"),
      partitionPath.day("Day=00"),
      partitionPath.day("Day=32"),
      partitionPath.hour("Hour=24"),
      partitionPath.minute("Minute=60")
    )

    assert(invalid.forall(_.isEmpty))
  }

  test("4.parsers require exact widths, numeric input, and no trailing content") {
    val invalid = List(
      partitionPath.year("Year=123"),
      partitionPath.year("Year=10000"),
      partitionPath.month("Month=1"),
      partitionPath.day("Day=abc"),
      partitionPath.hour("Hour=01-extra"),
      partitionPath.minute("Minute= 01")
    )

    assert(invalid.forall(_.isEmpty))
  }

  test("5.field names are case-insensitive but otherwise exact") {
    assertEquals(partitionPath.year("year=2026"), Some(2026))
    assertEquals(partitionPath.month("mOnTh=03"), Some(3))
    assertEquals(partitionPath.day("DAY=04"), Some(4))
    assertEquals(partitionPath.hour("hour=05"), Some(5))
    assertEquals(partitionPath.minute("minute=06"), Some(6))
    assertEquals(partitionPath.year("Years=2026"), None)
  }
}
