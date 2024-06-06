package mtest.terminals

import cats.kernel.Eq
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import org.scalatest.funsuite.AnyFunSuite

import java.time.{LocalDate, LocalDateTime}

class NJPathTest extends AnyFunSuite {
  test("local") {
    val r1: NJPath = NJPath("./data/abc") / "efg"
    assert(r1.pathStr == "data/abc/efg")
    val r2: NJPath = NJPath("./data/abc/") / "efg"
    assert(r2.pathStr == "data/abc/efg")
    assert(Eq[NJPath].eqv(r1, r2))
    assert(r1.uri == r2.uri)
  }

  test("local absolute") {
    val r1: NJPath = NJPath("/data/abc") / "efg"
    assert(r1.pathStr == "/data/abc/efg")
    val r2: NJPath = NJPath("/data/abc/") / "/efg"
    assert(r2.pathStr == "/data/abc/efg")
    assert(Eq[NJPath].eqv(r1, r2))
  }

  test(". ..") {
    val r1: NJPath = NJPath("ftp://data/abc") / ".." / "efg"
    assert(r1.pathStr == "ftp://data/efg")
    val r2: NJPath = NJPath("ftp://data/abc/") / "." / "efg"
    assert(r2.pathStr == "ftp://data/abc/efg")
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
    val r3: NJPath = NJPath("s3a://bucket/folder") / "a_b_c" / "efg.json"
    assert(r3.pathStr == "s3a://bucket/folder/a_b_c/efg.json")
  }
  test("local date") {
    val r1: NJPath = NJPath("s3a://bucket")
    val ld         = LocalDate.of(2020, 1, 1)
    val r2: NJPath = r1 / ld / "deflate-1"
    assert(r2.pathStr == "s3a://bucket/Year=2020/Month=01/Day=01/deflate-1")
  }

  test("local date time") {
    val r1: NJPath = NJPath("s3a://bucket")
    val ld         = LocalDateTime.of(2020, 1, 1, 0, 0, 0, 0)
    val r2: NJPath = r1 / ld / 32 / "abc.json"
    assert(r2.pathStr == "s3a://bucket/Year=2020/Month=01/Day=01/Hour=00/0032/abc.json")
  }

  test("json") {
    val r1 = NJPath("s3a://bucket/folder/") / "/a/" / "/b" / "c/"
    val r2 = decode[NJPath](r1.asJson.noSpaces).toOption.get
    assert(r1.pathStr === r2.pathStr)
    assert(Eq[NJPath].eqv(r1, r2))
  }

  test("no space") {
    val r1: NJPath = NJPath("s3a://bucket/") / "  " / " abc "
    assert(r1.pathStr == "s3a://bucket/abc")
  }
}
