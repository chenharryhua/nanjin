package mtest.terminals

import cats.data.NonEmptyList
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import cats.implicits.toTraverseOps
import com.github.chenharryhua.nanjin.common.chrono.Policy
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopParquet, NJFileKind, ParquetFile}
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
import scala.concurrent.duration.DurationInt
import scala.util.Try

class NJParquetTest extends AnyFunSuite {
  import HadoopTestData.*

  val parquet: HadoopParquet[IO] = hdp.parquet(pandaSchema)

  def fs2(path: Url, file: ParquetFile, data: Set[GenericRecord]): Assertion = {
    val tgt  = path / file.fileName
    val ts   = Stream.emits(data.toList).covary[IO].chunks
    val sink = parquet.updateWriter(_.withCompressionCodec(file.compression.codecName)).sink(tgt)
    hdp.delete(tgt).unsafeRunSync()
    val action =
      ts.through(sink).compile.drain >>
        parquet.source(tgt, 100).compile.toList.map(_.toList)
    assert(action.unsafeRunSync().toSet == data)
    val fileName = (file: NJFileKind).asJson.noSpaces
    assert(jawn.decode[NJFileKind](fileName).toOption.get == file)
    val size = ts.through(sink).fold(0)(_ + _).compile.lastOrError.unsafeRunSync()
    assert(size == data.size)
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
    parquet.source("./does/not/exist", 100)
    parquet.sink("./does/not/exist")
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = ParquetFile(Snappy)
    val processedSize = Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .chunks
      .through(parquet.rotateSink(Policy.fixedDelay(1.second), ZoneId.systemDefault())(t =>
        path / file.ymdFileName(t)))
      .fold(0L)((sum, v) => sum + v.value)
      .compile
      .lastOrError
      .unsafeRunSync()
    val size =
      hdp
        .dataFolders(path)
        .flatMap(_.flatTraverse(hdp.filesIn))
        .flatMap(_.traverse(parquet.updateReader(identity).source(_, 10).compile.toList.map(_.size)))
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
}
