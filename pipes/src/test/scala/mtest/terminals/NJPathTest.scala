package mtest.terminals

import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, ZoneId, ZonedDateTime}
class NJPathTest extends AnyFunSuite {
  test("local") {
    val r1: NJPath = NJPath("./data/abc") / "efg"
    assert(r1.pathStr == "data/abc/efg")
    val r2: NJPath = NJPath("./data/abc/") / "efg"
    assert(r2.pathStr == "data/abc/efg")
  }

  test("local absolute") {
    val r1: NJPath = NJPath("/data/abc") / "efg"
    assert(r1.pathStr == "/data/abc/efg")
    val r2: NJPath = NJPath("/data/abc/") / "efg"
    assert(r2.pathStr == "/data/abc/efg")
  }
  test("norm") {
    val r1: NJPath = NJPath("s3a://bucket/folder")
    assert(r1.pathStr == "s3a://bucket/folder")
    val r2: NJPath = NJPath("s3a://bucket/folder/")
    assert(r2.pathStr == "s3a://bucket/folder/")
  }
  test("s3a") {
    val r1: NJPath = NJPath("s3a://bucket/folder")
    assert(r1.pathStr == "s3a://bucket/folder")
    val r2: NJPath = NJPath("s3a://bucket/folder/") / "abc"
    assert(r2.pathStr == "s3a://bucket/folder/abc")
    val r3: NJPath = NJPath("s3a://bucket/folder") / "abc" / "efg"
    assert(r3.pathStr == "s3a://bucket/folder/abc/efg")
  }
  test("local date") {
    val r1: NJPath = NJPath("s3a://bucket")
    val ld         = LocalDate.of(2020, 1, 1)
    val r2: NJPath = r1 / ld / "abc.json"
    assert(r2.pathStr == "s3a://bucket/Year=2020/Month=01/Day=01/abc.json")
  }
  test("zoned date time") {
    val r1: NJPath = NJPath("s3a://bucket")
    val ld         = ZonedDateTime.of(2020, 1, 1, 0, 0, 0, 0, ZoneId.of("Australia/Sydney"))
    val r2: NJPath = r1 / ld / "abc.json"
    assert(r2.pathStr == "s3a://bucket/Year=2020/Month=01/Day=01/Hour=00/abc.json")
  }
  test("illegal") {
    assertDoesNotCompile(""" NJPath("s3a://bucket/") / " " """)
    assertDoesNotCompile(""" NJPath("s3a://bucket/") / "" """)
    assertDoesNotCompile(""" NJPath("s3a://bucket/") / " a" """)
    assertDoesNotCompile(""" NJPath("s3a://bucket/") / "b " """)
    assertDoesNotCompile(""" NJPath("s3a://bucket/") / "a/b" """)

  }
}
