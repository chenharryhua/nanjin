package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.chrono.Policy
import fs2.Stream
import fs2.text.{lines, utf8}
import io.circe.generic.auto.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import squants.information.InformationConversions.InformationConversions

import java.time.ZoneId
import scala.concurrent.duration.DurationInt

class NJBytesTest extends AnyFunSuite {

  def fs2(path: Url, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts   = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(path).bytes
    val src  = hdp.source(path).bytes(64.bytes)
    val action = ts
      .map(_.asJson.noSpaces)
      .intersperse(System.lineSeparator())
      .through(utf8.encode)
      .through(sink)
      .compile
      .drain >>
      src.through(utf8.decode).through(lines).map(decode[Tiger](_)).rethrow.compile.toList
    assert(action.unsafeRunSync().toSet == data)
    assert(
      hdp
        .source(path)
        .bytes
        .through(utf8.decode)
        .through(lines)
        .map(decode[Tiger](_))
        .rethrow
        .compile
        .toList
        .unsafeRunSync()
        .toSet == data)

  }
  val fs2Root: Url = Url.parse("./data/test/terminals/bytes/fs2")

  test("uncompressed") {
    fs2(fs2Root / "tiger.json", TestData.tigerSet)
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
    hdp.source("./does/not/exist").bytes
    hdp.sink("./does/not/exist").bytes
  }

  test("rotation - tick") {
    val path   = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val sink =
      hdp
        .rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t => path / s"${t.index}.json")
        .bytes
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson.noSpaces)
      .intersperse(System.lineSeparator())
      .through(utf8.encode)
      .through(sink)
      .compile
      .drain
      .unsafeRunSync()
  }

  test("rotation - index") {
    val path   = fs2Root / "rotation" / "index"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val sink = hdp.rotateSink(t => path / s"${t.index}.json").bytes
    Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunkN(10000)
      .unchunks
      .map(_.asJson.noSpaces)
      .intersperse(System.lineSeparator())
      .through(utf8.encode)
      .through(sink)
      .compile
      .drain
      .unsafeRunSync()
  }
}
