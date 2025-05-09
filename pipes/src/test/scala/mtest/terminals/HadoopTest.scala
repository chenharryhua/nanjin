package mtest.terminals

import better.files.*
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.codec.year_month_day_hour
import com.github.chenharryhua.nanjin.terminals.toHadoopPath
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class HadoopTest extends AnyFunSuite {
  private val path: Url = Url.parse("./data/test/terminals/hadoop")

  private val p1 = path / year_month_day_hour(LocalDateTime.of(2023, 8, 28, 2, 0, 0)) / "a.txt"
  private val p2 = path / year_month_day_hour(LocalDateTime.of(2023, 8, 29, 1, 0, 0)) / "b.txt"
  private val p3 = path / year_month_day_hour(LocalDateTime.of(2023, 8, 30, 0, 0, 0)) / "c.txt"
  private val p4 = path / year_month_day_hour(LocalDateTime.of(2023, 8, 30, 1, 0, 0)) / "d.txt"

  File(p1.toString()).createFileIfNotExists(createParents = true)
  File(p2.toString()).createFileIfNotExists(createParents = true)
  File(p3.toString()).createFileIfNotExists(createParents = true)
  File(p4.toString()).createFileIfNotExists(createParents = true)

  test("ymd") {
    assert(hdp.earliestYmd(path).unsafeRunSync().get.toString().takeRight(25) === "Year=2023/Month=08/Day=28")
    assert(hdp.latestYmd(path).unsafeRunSync().get.toString().takeRight(25) === "Year=2023/Month=08/Day=30")
  }
  test("ymdh") {
    assert(
      hdp
        .earliestYmdh(path)
        .unsafeRunSync()
        .get
        .toString()
        .takeRight(33) === "Year=2023/Month=08/Day=28/Hour=02")
    assert(
      hdp
        .latestYmdh(path)
        .unsafeRunSync()
        .get
        .toString()
        .takeRight(33) === "Year=2023/Month=08/Day=30/Hour=01")
  }
  test("exist") {
    assert(hdp.isExist(p1).unsafeRunSync())
    assert(hdp.locatedFileStatus(path).unsafeRunSync().count(_.isFile) == 4)
  }

  test("file in") {
    val files = hdp.filesIn(p1).unsafeRunSync()
    assert(files.size === 1)
    assert(files.head.toString().takeRight(5) === "a.txt")
  }

  test("delete") {
    val delAction = for {
      before <- hdp.isExist(p1)
      del <- hdp.delete(p1)
      after <- hdp.isExist(p1)
    } yield ((before, del, after))

    val (before, del, after) = delAction.unsafeRunSync()
    assert(before)
    assert(del)
    assert(!after)
  }

  test("toHadoopPath") {
    val p1 = Url.parse("abc/efg")
    val p2 = p1 / "hij" / "kml"
    assert(toHadoopPath(p2).toString == "abc/efg/hij/kml")
    val p3 = Url.parse("./abc/efg")
    val p4 = p3 / "hij" / "" / "kml"
    assert(toHadoopPath(p4).toString == "abc/efg/hij/kml")

    val p5 = Url.parse("s3://bucket/key")
    val p6 = p5 / "abc/efg"
    assert(toHadoopPath(p6).toString == "s3a://bucket/key/abc/efg")

    val p7 = Url.parse("s3a://bucket/key/")
    val p8 = p7 / 1 / 2 / 3
    assert(toHadoopPath(p8).toString == "s3a://bucket/key/1/2/3")

    val p9 = Url.parse("abc/efg/")
    assert(toHadoopPath(p9).toString == "abc/efg")
    hdp.sink(p9).outputStream
    hdp.source(p9).inputStream
  }
}
