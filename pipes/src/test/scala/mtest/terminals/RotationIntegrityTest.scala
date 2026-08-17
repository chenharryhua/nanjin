package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import fs2.Stream
import io.circe.Json
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.*

class RotationIntegrityTest extends AnyFunSuite {

  private val root: Url = Url.parse("./data/test/terminals/rotation-integrity")

  private def tigers(n: Int): List[Json] =
    List.fill(n)(Tiger(1, Some("zoo")).asJson)

  // ============================
  // RotateBySizeSink tests
  // ============================

  test("size: record count integrity - various sizes") {
    // The sum of recordCount across all rotated files must equal total input elements
    val cases = List(
      (7, 3L),
      (10, 10L),
      (100, 7L),
      (1, 1L),
      (13, 5L),
      (20, 20L),
      (50, 13L)
    )
    cases.foreach { case (totalElements, sizeLimit) =>
      val path = root / "size-integrity" / s"n${totalElements}_s$sizeLimit"
      hdp.delete(path).unsafeRunSync()

      val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

      val rotateFiles = Stream
        .emits(tigers(totalElements))
        .covary[IO]
        .through(sink)
        .compile
        .toList
        .unsafeRunSync()

      val totalRecords = rotateFiles.map(_.recordCount).sum
      assert(
        totalRecords == totalElements.toLong,
        s"n=$totalElements, sizeLimit=$sizeLimit: expected $totalElements records, got $totalRecords"
      )
    }
  }

  test("size: large chunk exceeding multiple size limits") {
    // A single chunk of 25 elements with sizeLimit=3 should produce ceil(25/3) = 9 files
    val path = root / "size-multi-split"
    hdp.delete(path).unsafeRunSync()

    val sizeLimit = 3L
    val totalElements = 25
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(tigers(totalElements))
      .covary[IO]
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    assert(rotateFiles.map(_.recordCount).sum == totalElements.toLong)
    // 8 files of 3 + 1 file of 1
    assert(rotateFiles.size == 9)
    assert(rotateFiles.init.forall(_.recordCount == 3))
    assert(rotateFiles.last.recordCount == 1)
  }

  test("size: sizeLimit = 1 gives one file per element") {
    val path = root / "size-limit-one"
    hdp.delete(path).unsafeRunSync()

    val totalElements = 5
    val sink = hdp.rotateSink(sydneyTime, 1L)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(tigers(totalElements))
      .covary[IO]
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    assert(rotateFiles.size == totalElements)
    assert(rotateFiles.forall(_.recordCount == 1))
  }

  test("size: empty stream produces single file with zero records") {
    val path = root / "size-empty"
    hdp.delete(path).unsafeRunSync()

    val sink = hdp.rotateSink(sydneyTime, 10L)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .empty
      .covaryAll[IO, Json]
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    // A file is always opened, so exactly one file with 0 records
    assert(rotateFiles.size == 1)
    assert(rotateFiles.head.recordCount == 0)
  }

  test("size: file URL uniqueness") {
    val path = root / "size-unique-urls"
    hdp.delete(path).unsafeRunSync()

    val sink = hdp.rotateSink(sydneyTime, 3L)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(tigers(12))
      .covary[IO]
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    val urls = rotateFiles.map(_.url)
    assert(urls.distinct.size == urls.size, "All file URLs must be unique")
  }

  // ============================
  // RotateByPolicySink tests
  // ============================

  test("policy: record count integrity") {
    val path = root / "policy-integrity"
    hdp.delete(path).unsafeRunSync()

    val totalElements = 100
    val sink =
      hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(50.millis))(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(tigers(totalElements))
      .covary[IO]
      .metered(10.millis) // spread elements over time to trigger rotation
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    val totalRecords = rotateFiles.map(_.recordCount).sum
    assert(
      totalRecords == totalElements.toLong,
      s"Expected $totalElements records, got $totalRecords across ${rotateFiles.size} files"
    )
  }

  test("policy: empty stream produces single file with zero records") {
    val path = root / "policy-empty"
    hdp.delete(path).unsafeRunSync()

    val sink =
      hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(100.millis))(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .empty
      .covaryAll[IO, Json]
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    assert(rotateFiles.size == 1)
    assert(rotateFiles.head.recordCount == 0)
  }

  test("policy: file URL uniqueness") {
    val path = root / "policy-unique-urls"
    hdp.delete(path).unsafeRunSync()

    val sink =
      hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(30.millis))(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(tigers(50))
      .covary[IO]
      .metered(5.millis)
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    val urls = rotateFiles.map(_.url)
    assert(urls.distinct.size == urls.size, "All file URLs must be unique")
  }
}
