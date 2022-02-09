package mtest.terminals

import akka.stream.Materializer
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

    val action = hdp.delete(path) >>
      ts.through(parquet.updateWriter(_.withCompressionCodec(CompressionCodecName.SNAPPY)).sink(path)).compile.drain >>
      parquet.source(path).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }

  test("gzip parquet write/read") {
    val path = NJPath("./data/test/devices/panda.gzip.parquet")
    val ts   = Stream.emits(pandas).covary[IO]

    val action = hdp.delete(path) >>
      ts.through(parquet.updateWriter(_.withCompressionCodec(CompressionCodecName.GZIP)).sink(path)).compile.drain >>
      parquet.source(path).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }
  test("uncompressed parquet write/read") {
    val path = NJPath("./data/test/devices/panda.uncompressed.parquet")
    val ts   = Stream.emits(pandas).covary[IO]
    val action =
      hdp.delete(path) >> ts.through(parquet.sink(path)).compile.drain >> parquet.source(path).compile.toList

    assert(action.unsafeRunSync() == pandas)
  }

  implicit val mat: Materializer = Materializer(akkaSystem)
  test("uncompressed parquet write/read akka") {
    val path  = NJPath("./data/test/devices/akka/panda.uncompressed.parquet")
    val ts    = Source(pandas)
    val write = IO.fromFuture(IO(ts.runWith(parquet.akka.sink(path))))
    val read  = IO.fromFuture(IO(parquet.akka.source(path).runFold(List.empty[GenericRecord])(_.appended(_))))
    val rst   = (hdp.delete(path) >> write >> read).unsafeRunSync()
    assert(rst == pandas)
  }
}
