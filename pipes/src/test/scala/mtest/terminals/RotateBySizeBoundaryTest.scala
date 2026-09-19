package mtest.terminals

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import fs2.Stream
import io.circe.generic.auto.*
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger

class RotateBySizeBoundaryTest extends CatsEffectSuite {

  private val root: Url = Url.parse("./data/test/terminals/rotate-boundary")

  test("size rotation - exact boundary does not create extra file") {
    // sizeLimit = 5, emit exactly 5 elements => should produce 1 file, not 2
    val path = root / "exact-boundary"

    val sizeLimit = 5L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val run = hdp.delete(path) >> Stream
      .emits(List.fill(5)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      // exactly sizeLimit elements should produce exactly 1 file
      assert(rotateFiles.size == 1)
      assert(rotateFiles.head.recordCount == 5)
    }
  }

  test("size rotation - one over boundary creates 2 files") {
    // sizeLimit = 5, emit 6 elements => should produce 2 files
    val path = root / "one-over"

    val sizeLimit = 5L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val run = hdp.delete(path) >> Stream
      .emits(List.fill(6)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      assert(rotateFiles.size == 2)
      assert(rotateFiles.head.recordCount == 5)
      assert(rotateFiles.last.recordCount == 1)
    }
  }

  test("size rotation - exact multiple of boundary creates correct files") {
    // sizeLimit = 3, emit 9 elements => should produce 3 files of 3 each
    val path = root / "exact-multiple"

    val sizeLimit = 3L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val run = hdp.delete(path) >> Stream
      .emits(List.fill(9)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      assert(rotateFiles.size == 3)
      assert(rotateFiles.forall(_.recordCount == 3))
    }
  }

  test("size rotation - single element under limit produces 1 file") {
    val path = root / "single"

    val sizeLimit = 10L
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val run = hdp.delete(path) >> Stream
      .emit(Tiger(1, Some("zoo")))
      .covary[IO]
      .map(_.asJson)
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      assert(rotateFiles.size == 1)
      assert(rotateFiles.head.recordCount == 1)
    }
  }

  test("size rotation - chunk crossing boundary (chunkN=3, sizeLimit=5)") {
    // With chunkN(3) and sizeLimit=5, a split must happen within a chunk
    val path = root / "chunk-cross"

    val sizeLimit = 5L
    val totalElements = 13
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val run = hdp.delete(path) >> Stream
      .emits(List.fill(totalElements)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .chunkN(3)
      .unchunks
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      // 13 / 5 = 2 full + 1 partial => 3 files (5, 5, 3)
      assert(rotateFiles.map(_.recordCount).sum == totalElements.toLong)
      assert(rotateFiles.size == 3)
      assert(rotateFiles(0).recordCount == 5)
      assert(rotateFiles(1).recordCount == 5)
      assert(rotateFiles(2).recordCount == 3)
    }
  }

  test("size rotation - chunk larger than sizeLimit (chunkN=10, sizeLimit=3)") {
    // A single chunk of 10 items with sizeLimit=3 forces multiple intra-chunk splits
    val path = root / "chunk-larger"

    val sizeLimit = 3L
    val totalElements = 10
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.circe").circe

    val run = hdp.delete(path) >> Stream
      .emits(List.fill(totalElements)(Tiger(1, Some("zoo"))))
      .covary[IO]
      .map(_.asJson)
      .chunkN(10)
      .unchunks
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      // 10 / 3 = 3 full + 1 partial => 4 files (3, 3, 3, 1)
      assert(rotateFiles.map(_.recordCount).sum == totalElements.toLong)
      assert(rotateFiles.size == 4)
      assert(rotateFiles.init.forall(_.recordCount == 3))
      assert(rotateFiles.last.recordCount == 1)
    }
  }

  test("size rotation - chunk boundary with text format (chunkN=7, sizeLimit=4)") {
    val path = root / "chunk-text"

    val sizeLimit = 4L
    val totalElements = 15
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.txt").text

    val run = hdp.delete(path) >> Stream
      .emits(List.fill(totalElements)("hello"))
      .covary[IO]
      .chunkN(7)
      .unchunks
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      // 15 / 4 = 3 full + 1 partial => 4 files (4, 4, 4, 3)
      assert(rotateFiles.map(_.recordCount).sum == totalElements.toLong)
      assert(rotateFiles.size == 4)
      assert(rotateFiles.init.forall(_.recordCount == 4))
      assert(rotateFiles.last.recordCount == 3)
    }
  }

  test("size rotation - chunk boundary with binAvro format (chunkN=5, sizeLimit=3)") {
    import mtest.terminals.HadoopTestData.*
    val path = root / "chunk-binavro"

    val sizeLimit = 3L
    val totalElements = 11
    val sink = hdp.rotateSink(sydneyTime, sizeLimit)(t => path / s"${t.index}.bin.avro").binAvro

    val run = hdp.delete(path) >> Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(totalElements)
      .take(totalElements.toLong)
      .chunkN(5)
      .unchunks
      .through(sink)
      .compile
      .toList

    run.map { rotateFiles =>
      // 11 / 3 = 3 full + 1 partial => 4 files (3, 3, 3, 2)
      assert(rotateFiles.map(_.recordCount).sum == totalElements.toLong)
      assert(rotateFiles.size == 4)
      assert(rotateFiles.init.forall(_.recordCount == 3))
      assert(rotateFiles.last.recordCount == 2)
    }
  }
}
