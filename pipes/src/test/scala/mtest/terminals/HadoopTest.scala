package mtest.terminals

import better.files.*
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import mtest.terminals.HadoopTestData.hdp
import org.scalatest.funsuite.AnyFunSuite

import java.time.LocalDateTime

class HadoopTest extends AnyFunSuite {
  private val path: NJPath = NJPath("./data/test/terminals/hadoop")

  private val p1 = path / LocalDateTime.of(2023, 8, 28, 2, 0, 0) / "a.txt"
  private val p2 = path / LocalDateTime.of(2023, 8, 29, 1, 0, 0) / "b.txt"
  private val p3 = path / LocalDateTime.of(2023, 8, 30, 0, 0, 0) / "c.txt"
  private val p4 = path / LocalDateTime.of(2023, 8, 30, 1, 0, 0) / "d.txt"

  File(p1.pathStr).createFileIfNotExists(createParents = true)
  File(p2.pathStr).createFileIfNotExists(createParents = true)
  File(p3.pathStr).createFileIfNotExists(createParents = true)
  File(p4.pathStr).createFileIfNotExists(createParents = true)

  test("ymd") {
    assert(hdp.earliestYmd(path).unsafeRunSync().get.pathStr.takeRight(25) === "Year=2023/Month=08/Day=28")
    assert(hdp.latestYmd(path).unsafeRunSync().get.pathStr.takeRight(25) === "Year=2023/Month=08/Day=30")
  }
  test("ymdh") {
    assert(
      hdp
        .earliestYmdh(path)
        .unsafeRunSync()
        .get
        .pathStr
        .takeRight(33) === "Year=2023/Month=08/Day=28/Hour=02")
    assert(
      hdp.latestYmdh(path).unsafeRunSync().get.pathStr.takeRight(33) === "Year=2023/Month=08/Day=30/Hour=01")
  }
  test("exist") {
    assert(hdp.isExist(p1).unsafeRunSync())
    assert(hdp.locatedFileStatus(path).unsafeRunSync().count(_.isFile) == 4)
  }

  test("file in") {
    val files = hdp.filesIn(p1).unsafeRunSync()
    assert(files.size === 1)
    assert(files.head.pathStr.takeRight(5) === "a.txt")
  }
}
