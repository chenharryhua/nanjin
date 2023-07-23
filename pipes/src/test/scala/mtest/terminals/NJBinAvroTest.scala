package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.common.NJFileFormat
import com.github.chenharryhua.nanjin.terminals.NJCompression.*
import com.github.chenharryhua.nanjin.terminals.NJPath
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

  def fs2(path: NJPath, data: Set[GenericRecord]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val sink   = binAvro.sink(path)
    val src    = binAvro.source(path)
    val ts     = Stream.emits(data.toList).covary[IO]
    val action = ts.through(sink).compile.drain >> src.compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }

  val fs2Root: NJPath = NJPath("./data/test/terminals/bin_avro/panda")

  val fmt = NJFileFormat.BinaryAvro

  test("uncompressed") {
    fs2(fs2Root / Uncompressed.fileName(fmt), pandaSet)
  }

  test("gzip") {
    fs2(fs2Root / Gzip.fileName(fmt), pandaSet)
  }

  test("snappy") {
    fs2(fs2Root / Snappy.fileName(fmt), pandaSet)
  }

  test("bzip2") {
    fs2(fs2Root / Bzip2.fileName(fmt), pandaSet)
  }

  test("lz4") {
    fs2(fs2Root / Lz4.fileName(fmt), pandaSet)
  }

  test("deflate") {
    fs2(fs2Root / Deflate(1).fileName(fmt), pandaSet)
  }

  ignore("zstandard") {
    fs2(fs2Root / Zstandard(1).fileName(fmt), pandaSet)
  }

  test("laziness") {
    binAvro.source(NJPath("./does/not/exist"))
    binAvro.sink(NJPath("./does/not/exist"))
  }

  test("rotation") {
    val path   = fs2Root / "rotation"
    val number = 10000L
    hdp.delete(path).unsafeRunSync()
    Stream
      .emits(pandaSet.toList)
      .covary[IO]
      .repeatN(number)
      .through(binAvro.sink(RetryPolicies.constantDelay[IO](1.second))(t =>
        path / s"${t.index}.${Uncompressed.fileName(fmt)}"))
      .compile
      .drain
      .unsafeRunSync()
    val size = Stream.force(hdp.filesIn(path).map(binAvro.source)).compile.toList.map(_.size).unsafeRunSync()
    assert(size == number * 2)
  }
}
