package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.policies
import com.github.chenharryhua.nanjin.terminals.{NEWLINE_SEPARATOR, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import fs2.text.{lines, utf8}
import io.circe.generic.auto.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import squants.information.Bytes

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJBytesTest extends AnyFunSuite {

  def fs2(path: NJPath, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.bytes
      .withCompressionLevel(ZlibCompressor.CompressionLevel.BEST_SPEED)
      .withBufferSize(Bytes(8192))
      .withBlockSizeHint(1000)
      .sink(path)
    val src = hdp.bytes.source(path)
    val action = ts
      .map(_.asJson.noSpaces)
      .intersperse(NEWLINE_SEPARATOR)
      .through(utf8.encode)
      .chunks
      .through(sink)
      .compile
      .drain >>
      src.through(utf8.decode).through(lines).map(decode[Tiger](_)).rethrow.compile.toList
    assert(action.unsafeRunSync().toSet == data)

  }
  val fs2Root: NJPath = NJPath("./data/test/terminals/bytes/fs2")

  test("uncompressed") {
    fs2(fs2Root / "tiger.json", TestData.tigerSet)
  }

  test("input/output stream") {
    hdp.bytes
      .inputStream(fs2Root / "tiger.json")
      .flatMap(is => hdp.bytes.outputStream(fs2Root / "io.tiger.json").map(os => is.transferTo(os)))
      .use_
      .unsafeRunSync()
  }

  test("gzip") {
    fs2(fs2Root / "tiger.json.gz", TestData.tigerSet)
  }
  test("snappy") {
    fs2(fs2Root / "tiger.json.snappy", TestData.tigerSet)
  }
  test("bzip2") {
    fs2(fs2Root / "tiger.json.bz2", TestData.tigerSet)
  }
  test("lz4") {
    fs2(fs2Root / "tiger.json.lz4", TestData.tigerSet)
  }

  test("deflate") {
    fs2(fs2Root / "tiger.json.deflate", TestData.tigerSet)
  }

  ignore("ZSTANDARD") {
    fs2(fs2Root / "tiger.json.zst", TestData.tigerSet)
  }

  test("laziness") {
    hdp.bytes.source(NJPath("./does/not/exist"))
    hdp.bytes.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val sink =
      hdp.bytes.sink(policies.fixedDelay(1.second), ZoneId.systemDefault())(t => path / s"${t.index}.byte")
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson.noSpaces)
      .intersperse(NEWLINE_SEPARATOR)
      .through(utf8.encode)
      .chunks
      .through(sink)
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream
      .force(hdp.filesIn(path).map(hdp.bytes.source))
      .through(utf8.decode)
      .through(lines)
      .compile
      .toList
      .map(_.size)
      .unsafeRunSync()
    assert(size == number * 10)
  }
}
