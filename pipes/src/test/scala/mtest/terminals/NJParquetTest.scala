package mtest.terminals

import akka.stream.scaladsl.Source
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
  def akka(path: NJPath, name: CompressionCodecName, data: Set[GenericRecord]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val sink = parquet.updateWriter(_.withCompressionCodec(name)).akka.sink(path)
    val src  = parquet.akka.source(path)
    val ts   = Source(data)
    val rst =
      IO.fromFuture(IO(ts.runWith(sink))) >> IO.fromFuture(IO(src.runFold(Set.empty[GenericRecord])(_ + _)))
    assert(rst.unsafeRunSync() == data)
  }

  val akkaRoot: NJPath = NJPath("./data/test/terminals/parquet/akka")
  val fs2Root: NJPath  = NJPath("./data/test/terminals/parquet/fs2")

  test("parquet snappy") {
    fs2(fs2Root / "panda.snappy.parquet", NJCompression.Snappy.codecName, pandaSet)
    akka(akkaRoot / "panda.snappy.parquet", CompressionCodecName.SNAPPY, pandaSet)
  }
  test("parquet gzip") {
    fs2(fs2Root / "panda.gzip.parquet", NJCompression.Gzip.codecName, pandaSet)
    akka(akkaRoot / "panda.gzip.parquet", CompressionCodecName.GZIP, pandaSet)
  }

  test("uncompressed parquet") {
    fs2(fs2Root / "panda.uncompressed.parquet", CompressionCodecName.UNCOMPRESSED, pandaSet)
    akka(akkaRoot / "panda.uncompressed.parquet", CompressionCodecName.UNCOMPRESSED, pandaSet)
  }

  test("LZ4 parquet") {
    fs2(fs2Root / "panda.LZ4.parquet", CompressionCodecName.LZ4, pandaSet)
    akka(akkaRoot / "panda.LZ4.parquet", CompressionCodecName.LZ4, pandaSet)
  }

  test("ZSTD parquet") {
    fs2(fs2Root / "panda.ZSTD.parquet", CompressionCodecName.ZSTD, pandaSet)
    akka(akkaRoot / "panda.ZSTD.parquet", CompressionCodecName.ZSTD, pandaSet)
  }

  ignore("LZO parquet") {
    fs2(fs2Root / "panda.LZO.parquet", CompressionCodecName.LZO, pandaSet)
    akka(akkaRoot / "panda.LZO.parquet", CompressionCodecName.LZO, pandaSet)
  }

  ignore("BROTLI parquet") {
    fs2(fs2Root / "panda.BROTLI.parquet", CompressionCodecName.BROTLI, pandaSet)
    akka(akkaRoot / "panda.BROTLI.parquet", CompressionCodecName.BROTLI, pandaSet)
  }

}
