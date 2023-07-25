package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.{BinAvroFile, NJPath}
import eu.timepit.refined.auto.*
import fs2.Stream
import org.apache.avro.generic.GenericRecord
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import retry.RetryPolicies

import scala.concurrent.duration.DurationInt

class NJBinAvroTest extends AnyFunSuite {
  import HadoopTestData.*

  val binAvro = hdp.binAvro(pandaSchema)

  def fs2(path: NJPath, file: BinAvroFile, data: Set[GenericRecord]): Assertion = {
    val tgt = path / file.fileName

    hdp.delete(tgt).unsafeRunSync()
    val sink   = binAvro.withCompressionLevel(file.compression.compressionLevel).sink(tgt)
    val src    = binAvro.source(tgt)
    val ts     = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/bin_avro/panda")

  test("uncompressed") {
    fs2(fs2Root, BinAvroFile(Uncompressed), pandaSet)
  }

  test("gzip") {
    fs2(fs2Root, BinAvroFile(Gzip), pandaSet)
  }

  test("snappy") {
    fs2(fs2Root, BinAvroFile(Snappy), pandaSet)
  }

  test("bzip2") {
    fs2(fs2Root, BinAvroFile(Bzip2), pandaSet)
  }

  test("lz4") {
    fs2(fs2Root, BinAvroFile(Lz4), pandaSet)
  }

  test("deflate - 0") {
    fs2(fs2Root / 0, BinAvroFile(Deflate(0)), pandaSet)
  }
  test("deflate - 1") {
    fs2(fs2Root / 1, BinAvroFile(Deflate(1)), pandaSet)
  }
  test("deflate - -1") {
    fs2(fs2Root / -1, BinAvroFile(Deflate(-1)), pandaSet)
  }

  test("laziness") {
    binAvro.source(NJPath("./does/not/exist"))
    binAvro.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    val file = BinAvroFile(Uncompressed)
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(binAvro.sink(RetryPolicies.constantDelay[IO](1.second))(t => path / file.rotate(t)))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(binAvro.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
