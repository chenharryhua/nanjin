package mtest.terminals

import akka.stream.scaladsl.Source
import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.GenericRecord
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.scalatest.funsuite.AnyFunSuite

class ParquetTest extends AnyFunSuite {
  import HadoopTestData.*

  val parquet = hdp.parquet(pandaSchema)

  test("snappy parquet write/read") {
    import HadoopTestData.*
    val path = NJPath("./data/test/devices/builder/panda.snappy.parquet")
    val ts   = Stream.emits(pandas).covary[IO]
    hdp.delete(path).unsafeRunSync()
    val action =
      ts.through(parquet.updateWriter(_.withCompressionCodec(CompressionCodecName.SNAPPY)).sink(path)).compile.drain >>
        parquet.source(path).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }

  test("gzip parquet write/read") {
    val path = NJPath("./data/test/devices/panda.gzip.parquet")
    val ts   = Stream.emits(pandas).covary[IO]
    hdp.delete(path).unsafeRunSync()

    val action =
      ts.through(parquet.updateWriter(_.withCompressionCodec(CompressionCodecName.GZIP)).sink(path)).compile.drain >>
        parquet.source(path).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }
  test("uncompressed parquet write/read") {
    val path = NJPath("./data/test/devices/panda.uncompressed.parquet")
    hdp.delete(path).unsafeRunSync()
    val ts   = Stream.emits(pandas).covary[IO]
    val src  = parquet.source(path)
    val sink = parquet.sink(path)
    ts.through(sink).compile.drain.unsafeRunSync()
    val action = src.compile.toList
    assert(action.unsafeRunSync() == pandas)
  }

  test("uncompressed parquet write/read akka") {
    val path = NJPath("./data/test/devices/akka/panda.uncompressed.parquet")
    hdp.delete(path).unsafeRunSync()
    val sink = parquet.akka.sink(path)
    val src  = parquet.akka.source(path)
    val ts   = Source(pandas)
    IO.fromFuture(IO(ts.runWith(sink))).unsafeRunSync()
    val rst = IO.fromFuture(IO(src.runFold(List.empty[GenericRecord])(_.appended(_)))).unsafeRunSync()
    assert(rst == pandas)
  }
}
