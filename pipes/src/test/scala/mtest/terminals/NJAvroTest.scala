package mtest.terminals

import cats.effect.IO
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import org.apache.avro.generic.GenericRecord

import java.time.ZoneId
import scala.concurrent.duration.*

class NJAvroTest extends CatsEffectSuite {
  import HadoopTestData.*

  def fs2(path: Url, file: AvroFile, data: Set[GenericRecord]): IO[Unit] = {
    val tgt = path / file.fileName
    val sink = hdp.sink(tgt).avro(file.compression)
    val src = hdp.source(tgt).avro(100)
    val ts = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList.map(_.toList)
    val fileName = (file: FileKind).asJson.noSpaces

    for {
      _ <- hdp.delete(tgt)
      _ = assert(jawn.decode[FileKind](fileName).toOption.get == file)
      actionResult <- action
      _ = assert(actionResult.toSet == data)
      size <- ts.through(sink).fold(0)(_ + _).compile.lastOrError
      _ <- hdp.source(tgt).avro(100, readerSchema).debug().compile.drain
      _ = assert(size == data.size)
      roundTrip <- hdp.source(tgt).avro(100).compile.toList
    } yield assert(roundTrip.toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/avro/panda")

  test("1.snappy avro") {
    fs2(fs2Root, AvroFile(_.Snappy), pandaSet)
  }

  test("2.deflate 6 avro") {
    fs2("data/test/terminals/avro/panda", AvroFile(_.Deflate(_.Six)), pandaSet)
  }

  test("3.uncompressed avro") {
    fs2(fs2Root, AvroFile(_.Uncompressed), pandaSet)
  }

  test("4.xz 1 avro") {
    fs2(fs2Root, AvroFile(_.Xz(_.One)), pandaSet)
  }

  test("5.bzip2 avro") {
    fs2(fs2Root, AvroFile(_.Bzip2), pandaSet)
  }

  test("6.zstandard avro") {
    fs2(fs2Root, AvroFile(_.Zstandard(_.One)), pandaSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").avro(100)
    hdp.sink("./does/not/exist").avro(_.Uncompressed)
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    val file = AvroFile(_.Uncompressed)
    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(pandaSet.toList)
        .covary[IO]
        .repeatN(number)
        .through(hdp.rotateSink(sydneyTime, _.fixedDelay(0.1.second).repeat)(t =>
          path / file.fileName(t)).avro(_.Uncompressed))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).avro(100).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = AvroFile(_.Uncompressed)
    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(pandaSet.toList)
        .covary[IO]
        .repeatN(number)
        .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).avro(_.Uncompressed))
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .filesIn(path)
          .flatMap(_.traverse(hdp.source(_).avro(100).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
  }

  test("10.stream concat") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "data.avro"

    for {
      _ <- hdp.delete(path) >>
        (s ++ s ++ s).through(hdp.sink(path).avro).compile.drain
      size <- hdp.source(path).avro(100).compile.fold(0) { case (s, _) => s + 1 }
    } yield assert(size == 3000)
  }

  test("11.stream concat - 2") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(ZoneId.systemDefault(), _.fixedDelay(0.1.second).repeat)(t =>
      path / AvroFile(_.Uncompressed).fileName(t))

    hdp.delete(path) >>
      (s ++ s ++ s).through(sink.avro).compile.drain
  }

  test("large number (10000) of files - passed but too cost to run it".ignore) {
    val path = fs2Root / "rotation" / "many"
    val number = 5000L
    val file = AvroFile(_.Uncompressed)
    hdp.delete(path) >> Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).avro(_.Uncompressed))
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .void
  }
}
