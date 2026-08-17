package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.scalatest.funsuite.AnyFunSuite

class RotateBySizeBoundaryTest extends AnyFunSuite {

  private val root: Url = Url.parse("./data/test/terminals/rotate-boundary")

  test("size rotation - exact boundary does not create extra file") {
    // sizeLimit = 5, emit exactly 5 elements => should produce 1 file, not 2
    val path = root / "exact-boundary"
    hdp.delete(path).unsafeRunSync()

    val sizeLimit = 5L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(List.fill(5)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    // exactly sizeLimit elements should produce exactly 1 file
    assert(rotateFiles.size == 1)
    assert(rotateFiles.head.recordCount == 5)
  }

  test("size rotation - one over boundary creates 2 files") {
    // sizeLimit = 5, emit 6 elements => should produce 2 files
    val path = root / "one-over"
    hdp.delete(path).unsafeRunSync()

    val sizeLimit = 5L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(List.fill(6)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    assert(rotateFiles.size == 2)
    assert(rotateFiles.head.recordCount == 5)
    assert(rotateFiles.last.recordCount == 1)
  }

  test("size rotation - exact multiple of boundary creates correct files") {
    // sizeLimit = 3, emit 9 elements => should produce 3 files of 3 each
    val path = root / "exact-multiple"
    hdp.delete(path).unsafeRunSync()

    val sizeLimit = 3L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emits(List.fill(9)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    assert(rotateFiles.size == 3)
    assert(rotateFiles.forall(_.recordCount == 3))
  }

  test("size rotation - single element under limit produces 1 file") {
    val path = root / "single"
    hdp.delete(path).unsafeRunSync()

    val sizeLimit = 10L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val rotateFiles = Stream
      .emit(Tiger(1, Some("zoo")))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList
      .unsafeRunSync()

    assert(rotateFiles.size == 1)
    assert(rotateFiles.head.recordCount == 1)
  }
}
