package mtest.terminals

import cats.data.NonEmptyList
import cats.effect.IO
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.zones.sydneyTime
import com.github.chenharryhua.nanjin.terminals.{FileKind, ParquetFile}
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import munit.CatsEffectSuite
import org.apache.avro.generic.GenericRecord

import java.time.ZoneId
import scala.concurrent.duration.*
import scala.util.Try

class NJParquetTest extends CatsEffectSuite {
  import HadoopTestData.*
  val zoneId: ZoneId = ZoneId.systemDefault()

  def fs2(path: Url, file: ParquetFile, data: Set[GenericRecord]): IO[Unit] = {
    val tgt = path / file.fileName
    val ts = Stream.emits(data.toList).covary[IO]
    val sink = hdp.sink(tgt).parquet(_.withCompressionCodec(file.compression.codecName))
    val action =
      ts.through(sink).compile.drain >>
        hdp.source(tgt).parquet(100, _.useBloomFilter()).compile.toList.map(_.toList)
    val fileName = (file: FileKind).asJson.noSpaces
    for {
      _ <- hdp.delete(tgt)
      actionResult <- action
      _ = assert(actionResult.toSet == data)
      _ = assert(jawn.decode[FileKind](fileName).toOption.get == file)
      size <- ts.through(sink).fold(0)(_ + _).compile.lastOrError
      _ = assert(size == data.size)
      roundTrip <- hdp.source(tgt).parquet(100).compile.toList
    } yield assert(roundTrip.toSet == data)
  }

  val fs2Root: Url = Url.parse("./data/test/terminals/parquet/panda")

  test("1.parquet snappy") {
    fs2(fs2Root, ParquetFile(_.Snappy), pandaSet)
  }
  test("2.parquet gzip") {
    fs2(fs2Root, ParquetFile(_.Gzip), pandaSet)
  }

  test("3.uncompressed parquet") {
    fs2(fs2Root, ParquetFile(_.Uncompressed), pandaSet)
  }

  test("4.LZ4 parquet") {
    fs2(fs2Root, ParquetFile(_.Lz4), pandaSet)
  }

  test("5.LZ4_RAW parquet") {
    fs2(fs2Root, ParquetFile(_.Lz4Raw), pandaSet)
  }

  test("6.Zstandard parquet - 1") {
    fs2(fs2Root, ParquetFile(_.Zstandard(_.Seven)), pandaSet)
  }

  test("LZO parquet".ignore) {
    fs2(fs2Root, ParquetFile(_.Lzo), pandaSet)
  }

  test("BROTLI parquet".ignore) {
    fs2(fs2Root, ParquetFile(_.Brotli), pandaSet)
  }

  test("7.laziness") {
    hdp.source("./does/not/exist").parquet(100)
    hdp.sink("./does/not/exist").parquet
  }

  test("8.rotation - policy") {
    val path = fs2Root / "rotation" / "tick"
    val number = 10000L
    val file = ParquetFile(_.Snappy)
    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(pandaSet.toList)
        .covary[IO]
        .repeatN(number)
        .through(hdp.rotateSink(zoneId, _.fixedDelay(0.2.second).repeat)(t =>
          path / file.ymdFileName(t)).parquet)
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .dataFolders(path)
          .flatMap(_.flatTraverse(hdp.filesIn))
          .flatMap(_.traverse(hdp.source(_).parquet(10).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
  }

  test("9.rotation - size") {
    val path = fs2Root / "rotation" / "index"
    val number = 10000L
    val file = ParquetFile(_.Snappy)
    for {
      _ <- hdp.delete(path)
      processedSize <- Stream
        .emits(pandaSet.toList)
        .covary[IO]
        .repeatN(number)
        .through(hdp.rotateSink(sydneyTime, 1000)(t => path / file.fileName(t)).parquet)
        .fold(0L)((sum, v) => sum + v.recordCount)
        .compile
        .lastOrError
      size <-
        hdp
          .dataFolders(path)
          .flatMap(_.flatTraverse(hdp.filesIn))
          .flatMap(_.traverse(hdp.source(_).parquet(10).compile.toList.map(_.size)))
          .map(_.sum)
    } yield {
      assert(size == number * 2)
      assert(processedSize == number * 2)
    }
  }

  test("10.best") {
    val path = fs2Root / "rotation" / "tick"

    def r1(str: String): Option[Int] = Try(str.takeRight(4).toInt).toOption
    def r2(str: String): Option[Int] = Try(str.takeRight(2).toInt).toOption

    for {
      res1 <- hdp.latestYmd(path)
      res2 <- hdp.latestYmdh(path)
      res3 <- hdp.best(path, NonEmptyList.of(r1(_), r2(_)))
    } yield {
      assert(res1.nonEmpty)
      assert(res2.isEmpty)
      assert(res3.exists(_.toString().takeRight(8).take(6) == "Month="))
    }
  }

  test("11.stream concat") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "data.parquet"

    for {
      _ <- hdp.delete(path) >>
        (s ++ s ++ s).through(hdp.sink(path).parquet).compile.drain
      size <- hdp.source(path).parquet(100).compile.fold(0) { case (s, _) =>
        s + 1
      }
    } yield assert(size == 3000)
  }

  test("12.stream concat - 2") {
    val s = Stream.emits(pandaSet.toList).covary[IO].repeatN(500)
    val path: Url = fs2Root / "concat" / "rotate"
    val sink =
      hdp.rotateSink(zoneId, _.fixedDelay(0.1.second).repeat)(t =>
        path / ParquetFile(_.Uncompressed).fileName(t))

    hdp.delete(path) >>
      (s ++ s ++ s).through(sink.parquet).compile.drain
  }

  test("large number (10000) of files - passed but too cost to run it".ignore) {
    val path = fs2Root / "rotation" / "many"
    val number = 5000L
    val file = ParquetFile(_.Uncompressed)
    hdp.delete(path) >> Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(hdp.rotateSink(sydneyTime, 1)(t => path / file.fileName(t)).parquet)
      .fold(0L)((sum, v) => sum + v.recordCount)
      .compile
      .lastOrError
      .void
  }
}
