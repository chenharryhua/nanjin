package mtest.terminals

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.{FileKind, ParquetFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.jawn
import io.circe.syntax.EncoderOps
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.*
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

import java.time.ZoneId
import scala.concurrent.duration.{DurationDouble, DurationInt}
import scala.util.Try

class NJParquetTest extends AnyFunSuite {
  import HadoopTestData.*

  def fs2(path: Url, file: ParquetFile, data: Set[GenericRecord]): Assertion = {
    val tgt  = path / file.fileName
    val ts   = Stream.emits(data.toList).covary[IO].chunks
    val sink = hdp.sink(tgt).parquet(_.withCompressionCodec(file.compression.codecName))
    hdp.delete(tgt).unsafeRunSync()
    val action =
      ts.through(sink).compile.drain >>
        hdp.source(tgt).parquet(100, _.useBloomFilter()).compile.toList.map(_.toList)
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: FileKind).asJson.noSpaces
    assert(jawn.decode[FileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
    assert(hdp.source(tgt).parquet(100).compile.toList.unsafeRunSync().toSet == data)
  }

  val fs2Root: Url = Url("./data/test/terminals/parquet/panda")

  test("parquet snappy") {
    fs2(fs2Root, ParquetFile(_.Snappy), pandaSet)
  }
  test("parquet gzip") {
    fs2(fs2Root, ParquetFile(_.Gzip), pandaSet)
  }

  test("uncompressed parquet") {
    fs2(fs2Root, ParquetFile(_.Uncompressed), pandaSet)
  }

  test("LZ4 parquet") {
    fs2(fs2Root, ParquetFile(_.Lz4), pandaSet)
  }

  test("LZ4_RAW parquet") {
    fs2(fs2Root, ParquetFile(_.Lz4_Raw), pandaSet)
  }

  test("Zstandard parquet - 1") {
    fs2(fs2Root, ParquetFile(_.Zstandard(7)), pandaSet)
  }

  ignore("LZO parquet") {
    fs2(fs2Root, ParquetFile(_.Lzo), pandaSet)
  }

  ignore("BROTLI parquet") {
    fs2(fs2Root, ParquetFile(_.Brotli), pandaSet)
  }

  test("laziness") {
    hdp.source("./does/not/exist").parquet(100)
    hdp.sink("./does/not/exist").parquet
  }

  test("rotation") {
    val path   = fs2Root / "rotation" / "tick"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = ParquetFile(_.Snappy)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunks
      .through(hdp
        .rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t => path / file.ymdFileName(t))
        .parquet)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .dataFolders(path)
        .flatMap(_.flatTraverse(hdp.filesIn))
        .flatMap(_.traverse(hdp.source(_).parquet(10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("rotation - index") {
    val path   = fs2Root / "rotation" / "index"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunkN(1000)
      .through(hdp.rotateSink(t => path / s"$t.parquet").parquet)
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .dataFolders(path)
        .flatMap(_.flatTraverse(hdp.filesIn))
        .flatMap(_.traverse(hdp.source(_).parquet(10).compile.toList.map(_.size)))
        .map(_.sum)
        .unsafeRunSync()
    assert(size == number * 2)
    assert(processedSize == number * 2)
  }

  test("best") {
    val path = fs2Root / "rotation"
    val res1 = hdp.latestYmd(path).unsafeRunSync()
    val res2 = hdp.latestYmdh(path).unsafeRunSync()
    assert(res1.nonEmpty)
    assert(res2.isEmpty)

    def r1(str: String): Option[Int] = Try(str.takeRight(4).toInt).toOption
    def r2(str: String): Option[Int] = Try(str.takeRight(2).toInt).toOption

    val res3 = hdp.best(path, NonEmptyList.of(r1, r2)).unsafeRunSync()
    assert(res3.exists(_.toString().takeRight(8).take(6) === "Month="))
  }

  test("stream concat") {
    val s         = Stream.emits(pandaSet.toList).covary[IO].repeatN(500).chunks
    val path: Url = fs2Root / "concat" / "data.parquet"

    (hdp.delete(path) >>
      (s ++ s ++ s).through(hdp.sink(path).parquet).compile.drain).unsafeRunSync()
    val size = hdp.source(path).parquet(100).compile.fold(0) { case (s, _) => s + 1 }.unsafeRunSync()
    assert(size == 3000)
  }

  test("stream concat - 2") {
    val s         = Stream.emits(pandaSet.toList).covary[IO].repeatN(500).chunks
    val path: Url = fs2Root / "concat" / "rotate"
    val sink = hdp.rotateSink(Policy.fixedDelay(0.1.second), ZoneId.systemDefault())(t =>
      path / ParquetFile(_.Uncompressed).fileName(t))

    (hdp.delete(path) >>
      (s ++ s ++ s).through(sink.parquet).compile.drain).unsafeRunSync()
  }
}
