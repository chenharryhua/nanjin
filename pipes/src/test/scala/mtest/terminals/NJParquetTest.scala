package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{HadoopParquet, NJPath, ParquetFile}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt

class NJParquetTest extends AnyFunSuite {
  import HadoopTestData.*

  val parquet: HadoopParquet[IO] = hdp.parquet(pandaSchema)

  def fs2(path: NJPath, file: ParquetFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName
    val ts  = Stream.emits(data.toList).covary[IO]
    hdp.delete(tgt).unsafeRunSync()
    val action =
      ts.through(parquet.updateWriter(_.withCompressionCodec(file.compression.codecName)).sink(tgt))
        .compile
        .drain >>
        parquet.source(tgt).compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/parquet/panda")

  test("parquet snappy") {
    fs2(fs2Root, ParquetFile(Snappy), pandaSet)
  }
  test("parquet gzip") {
    fs2(fs2Root, ParquetFile(Gzip), pandaSet)
  }

  test("uncompressed parquet") {
    fs2(fs2Root, ParquetFile(Uncompressed), pandaSet)
  }

  test("LZ4 parquet") {
    fs2(fs2Root, ParquetFile(Lz4), pandaSet)
  }

  test("LZ4_RAW parquet") {
    fs2(fs2Root, ParquetFile(Lz4_Raw), pandaSet)
  }

  test("Zstandard parquet - 1") {
    fs2(fs2Root / 1, ParquetFile(Zstandard(1)), pandaSet)
  }

  ignore("LZO parquet") {
    fs2(fs2Root, ParquetFile(Lzo), pandaSet)
  }

  ignore("BROTLI parquet") {
    fs2(fs2Root, ParquetFile(Brotli), pandaSet)
  }

  test("laziness") {
    parquet.source(NJPath("./does/not/exist"))
    parquet.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = ParquetFile(Snappy)
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(parquet.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / file.rotate(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(parquet.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
