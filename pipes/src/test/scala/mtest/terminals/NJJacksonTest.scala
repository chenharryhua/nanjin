package mtest.terminals
import cats.effect.IO
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.{FileKind, JacksonFile}
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import org.apache.avro.generic.GenericRecord

import java.time.ZoneId
import scala.concurrent.duration.*
class NJJacksonTest extends CatsEffectSuite {
  import HadoopTestData.*

  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: JacksonFile, data: Set[GenericRecord]): IO[Unit] = {
    val tgt = path / file.fileName
    val sink = hdp.sink(tgt).jackson
    val src = hdp.source(tgt).jackson(10, pandaSchema)
    val ts = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    val fileName = (file: FileKind).asJson.noSpaces
    for {
      _ <- hdp.delete(tgt)
      actionResult <- action
      _ = assert(actionResult.toSet == data)
      _ = assert(jawn.decode[FileKind](fileName).toOption.get == file)
      size <- ts.through(sink).fold(0)(_ + _).compile.lastOrError
      _ <- hdp.source(tgt).jackson(100, pandaSchema, readerSchema).debug().compile.drain
      _ = assert(size == data.size)
      roundTrip <- hdp.source(tgt).jackson(10, pandaSchema).compile.toList
    } yield assert(roundTrip.toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/jackson/panda")
  test("1.uncompressed") {
    fs2(fs2Root, JacksonFile(_.Uncompressed), pandaSet)
  }

  test("2.gzip") {
    fs2(fs2Root, JacksonFile(_.Gzip), pandaSet)
  }

  test("3.snappy") {
    fs2(fs2Root, JacksonFile(_.Snappy), pandaSet)
  }

  test("4.bzip2") {
    fs2(fs2Root, JacksonFile(_.Bzip2), pandaSet)
  }

  test("5.lz4") {
    fs2(fs2Root, JacksonFile(_.Lz4), pandaSet)
  }

  test("6.deflate - 1") {
    fs2(fs2Root, JacksonFile(_.Deflate(_.Five)), pandaSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").jackson(10, pandaSchema)
    hdp.sink("./does/not/exist").jackson
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    val file = JacksonFile(_.Uncompressed)
    for {
      _ <- hdp.delete(path)
      processed <- Stream
        .emits(pandaSet.toList)
        .covary[IO]
        .repeatN(number)
        .through(hdp.rotateSink(zoneId, _.fixedDelay(200.millis).repeat)(t =>
          path / file.fileName(t)).jackson)
        .debug(_.asJson.noSpaces)
        .compile
        .toList
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).jackson(10, pandaSchema).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processed.map(_.recordCount).sum == number * 2)
      assert(processed.map(_.create.index).sliding(2).map(lst => lst(1) - lst.head).forall(_ == 1))
      assert(processed.map(_.url).distinct.size == processed.size)
      assert(processed.map(rf => rf.url.toString.contains(s"000${rf.create.index}")).forall(identity))
    }
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = JacksonFile(_.Uncompressed)
    val run = for {
      _ <- hdp.delete(path)
      tickedValues <- Stream
        .emits(pandaSet.toList)
        .covary[IO]
        .repeatN(number)
        .chunkN(300)
        .unchunks
        .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).jackson)
        .compile
        .toList
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).jackson(10, pandaSchema).compile.toList.map(_.size)))
          .map(_.sum)
    } yield (tickedValues, size)

    run.map { case (tickedValues, size) =>
      assert(size == number * 2)
      assert(tickedValues.map(_.recordCount).sum == number * 2)

      assert(tickedValues.head.recordCount == 1000)
      assert(tickedValues.head.url.path.parts.toList.last.contains("0001"))
      assert(tickedValues.head.create.index == 1)
      assert(tickedValues(1).recordCount == 1000)
      assert(tickedValues(1).url.path.parts.toList.last.contains("0002"))
      assert(tickedValues(1).create.index == 2)
      assert(tickedValues(2).recordCount == 1000)
      assert(tickedValues(2).url.path.parts.toList.last.contains("0003"))
      assert(tickedValues(2).create.index == 3)
      assert(tickedValues(3).recordCount == 1000)
      assert(tickedValues(3).create.index == 4)

      assert(tickedValues(4).recordCount == 1000)
      assert(tickedValues(4).create.index == 5)
      assert(tickedValues(5).recordCount == 1000)
      assert(tickedValues(5).create.index == 6)
      assert(tickedValues(6).recordCount == 1000)
      assert(tickedValues(6).create.index == 7)
      assert(tickedValues(7).recordCount == 1000)
      assert(tickedValues(7).create.index == 8)

      assert(tickedValues(8).recordCount == 1000)
      assert(tickedValues(8).create.index == 9)
      assert(tickedValues(9).recordCount == 1000)
      assert(tickedValues(9).create.index == 10)

      assert(tickedValues.last.recordCount == 1000)
    }
  }

  test("10.stream concat") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "jackson.json"

    for {
      _ <- hdp.delete(path) >>
        (s ++ s ++ s).through(hdp.sink(path).jackson).compile.drain
      size <-
        hdp.source(path).jackson(100, pandaSchema).compile.fold(0) { case (s, _) =>
          s + 1
        }
    } yield assert(size == 3000)
  }

  test("11.stream concat - 2") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink =
      hdp.rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t =>
        path / JacksonFile(_.Uncompressed).fileName(t))

    hdp.delete(path) >>
      (s ++ s ++ s).through(sink.jackson).compile.drain
  }

  test("12.timeout") {
    val path = fs2Root / "rotation" / "timeout"
    val number = 500000000L
    val file = JacksonFile(_.Uncompressed)
    val res = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(zoneId, _.fixedDelay(3.seconds).repeat)(t => path / file.fileName(t)).jackson)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .timeout(4.seconds)
      .compile
      .lastOrError
    for {
      _ <- hdp.delete(path)
      _ <- interceptIO[Throwable](res)
    } yield assert(better.files.File(path.toString()).list(_.extension.contains(".json")).size == 2)
  }

  test("large number (10000) of files - passed but too cost to run it".ignore) {
    val path = fs2Root / "rotation" / "many"
    val number = 5000L
    val file = JacksonFile(_.Uncompressed)
    hdp.delete(path) >> Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(sydneyTime, 1)(t => path / file.fileName(t)).jackson)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .void
  }
}
