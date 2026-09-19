package mtest.terminals

import cats.effect.IO
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import fs2.Stream
import fs2.text.{lines, utf8}
import io.circe.generic.auto.*
import io.circe.jawn.decode
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import mtest.terminals.HadoopTestData.hdp
import mtest.terminals.TestData.Tiger
import squants.information.InformationConversions.InformationConversions

import java.time.ZoneId
import scala.concurrent.duration.*

class NJBytesTest extends CatsEffectSuite {

  def fs2(path: Url, data: Set[Tiger]): IO[Unit] = {
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(path).bytes
    val src = hdp.source(path).bytes(64.bytes)
    val action = ts
      .map(_.asJson.noSpaces)
      .intersperse(System.lineSeparator())
      .through(utf8.encode)
      .through(sink)
      .compile
      .drain >>
      src.through(utf8.decode).through(lines).map(decode[Tiger](_)).rethrow.compile.toList
    for {
      _ <- hdp.delete(path)
      actionResult <- action
      _ = assert(actionResult.toSet == data)
      roundTrip <-
        hdp
          .source(path)
          .bytes(1.kb)
          .prefetchN(3)
          .chunks
          .map { c =>
            assert(c.nonEmpty)
            c
          }
          .unchunks
          .through(utf8.decode)
          .through(lines)
          .map(decode[Tiger](_))
          .rethrow
          .compile
          .toList
    } yield assert(roundTrip.toSet == data)
  }
  val fs2Root: Url = Url.parse("./data/test/terminals/bytes/fs2")

  test("1.uncompressed") {
    fs2(fs2Root / "tiger.json", TestData.tigerSet)
  }

  test("2.gzip") {
    fs2(fs2Root / "tiger.json.gz", TestData.tigerSet)
  }
  test("3.snappy") {
    fs2(fs2Root / "tiger.json.snappy", TestData.tigerSet)
  }
  test("4.bzip2") {
    fs2(fs2Root / "tiger.json.bz2", TestData.tigerSet)
  }
  test("5.lz4") {
    fs2(fs2Root / "tiger.json.lz4", TestData.tigerSet)
  }

  test("6.deflate") {
    fs2(fs2Root / "tiger.json.deflate", TestData.tigerSet)
  }

  test("ZSTANDARD".ignore) {
    fs2(fs2Root / "tiger.json.zst", TestData.tigerSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").bytes(1.mb)
    hdp.sink("./does/not/exist").bytes
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    val sink =
      hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.1.second).repeat)(t =>
        path / s"${t.index}.json").bytes
    hdp.delete(path) >> Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson.noSpaces)
      .intersperse(System.lineSeparator())
      .through(utf8.encode)
      .through(sink)
      .compile
      .drain
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val sink = hdp.rotateSink(sydneyTime, 10000)(t => path / s"${t.index}.json").bytes
    hdp.delete(path) >> Stream
      .emits(TestData.tigerSet.toList)
      .covary[IO]
      .repeatN(number)
      .map(_.asJson.noSpaces)
      .intersperse(System.lineSeparator())
      .through(utf8.encode)
      .through(sink)
      .compile
      .drain
  }
}
