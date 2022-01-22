package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.NJPath
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*

import java.time.{LocalDate, ZoneId, ZonedDateTime}
class NJPathTest extends AnyFunSuite {
  test("local") {
    val r1 = NJPath("./data/abc") / "efg"
    assert(r1.pathStr == "./data/abc/efg")
    val r2 = NJPath("./data/abc/") / "efg"
    assert(r2.pathStr == "./data/abc/efg")
  }
  test("s3a") {
    val r1 = NJPath("s3a://bucket/folder")
    assert(r1.pathStr == "s3a://bucket/folder")
    val r2 = NJPath("s3a://bucket/folder/") / "abc"
    assert(r2.pathStr == "s3a://bucket/folder/abc")
    val r3 = r1 / "abc.json"
    assert(r3.pathStr == "s3a://bucket/folder/abc.json")
  }
  test("local date") {
    val r1 = NJPath("s3a://bucket")
    val ld = LocalDate.of(2020, 1, 1)
    val r2 = r1 / ld / "abc.json"
    assert(r2.pathStr == "s3a://bucket/Year=2020/Month=01/Day=01/abc.json")
  }
  test("zoned date time") {
    val r1 = NJPath("s3a://bucket")
    val ld = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Australia/Sydney"))
    val r2 = r1 / ld / "abc.json"
    assert(r2.pathStr == "s3a://bucket/Year=2020/Month=01/Day=01/Hour=00/abc.json")
  }
}
