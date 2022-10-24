package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{NJCompression, NJParquet, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class NJParquetTest extends AnyFunSuite {
  import HadoopTestData.*

  val parquet: NJParquet[IO] = hdp.parquet(pandaSchema)

  def fs2(path: NJPath, name: CompressionCodecName, data: Set[GenericRecord]): Assertion = {
    val ts = Stream.emits(data.toList).covary[IO]
    hdp.delete(path).unsafeRunSync()
    val action =
      ts.through(parquet.updateWriter(_.withCompressionCodec(name)).sink(path)).compile.drain >>
        parquet.source(path).compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/parquet/fs2")

  test("parquet snappy") {
    fs2(fs2Root / "panda.snappy.parquet", NJCompression.Snappy.codecName, pandaSet)
  }
  test("parquet gzip") {
    fs2(fs2Root / "panda.gzip.parquet", NJCompression.Gzip.codecName, pandaSet)
  }

  test("uncompressed parquet") {
    fs2(fs2Root / "panda.uncompressed.parquet", CompressionCodecName.UNCOMPRESSED, pandaSet)
  }

  test("LZ4 parquet") {
    fs2(fs2Root / "panda.LZ4.parquet", CompressionCodecName.LZ4, pandaSet)
  }

  test("ZSTD parquet") {
    fs2(fs2Root / "panda.ZSTD.parquet", CompressionCodecName.ZSTD, pandaSet)
  }

  ignore("LZO parquet") {
    fs2(fs2Root / "panda.LZO.parquet", CompressionCodecName.LZO, pandaSet)
  }

  ignore("BROTLI parquet") {
    fs2(fs2Root / "panda.BROTLI.parquet", CompressionCodecName.BROTLI, pandaSet)
  }

  test("laziness") {
    parquet.source(NJPath("./does/not/exist"))
    parquet.sink(NJPath("./does/not/exist"))
  }

}
