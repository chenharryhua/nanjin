package mtest.terminals

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.CirceSerde
import com.github.chenharryhua.nanjin.terminals.NJPath
import eu.timepit.refined.auto.*
import fs2.Stream
import io.circe.generic.auto.*
import mtest.pipes.TestData
import mtest.pipes.TestData.Tiger
import mtest.terminals.HadoopTestData.hdp
import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.Assertion

class NJBytesTest extends AnyFunSuite {

  def fs2(path: NJPath, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts   = Stream.emits(data.toList).covary[IO]
    val sink = hdp.bytes.withCompressionLevel(ZlibCompressor.CompressionLevel.BEST_SPEED).sink(path)
    val src  = hdp.bytes.source(path)
    val action = ts.through(CirceSerde.toBytes(true)).through(sink).compile.drain >>
      src.through(CirceSerde.fromBytes[IO, Tiger]).compile.toList
    assert(action.unsafeRunSync().toSet == data)
  }
  val akkaRoot: NJPath = NJPath("./data/test/terminals/bytes/akka")
  val fs2Root: NJPath  = NJPath("./data/test/terminals/bytes/fs2")

  test("uncompressed") {
    fs2(fs2Root / "tiger.json", TestData.tigerSet)
  }

  test("gzip") {
    fs2(fs2Root / "tiger.json.gz", TestData.tigerSet)
  }
  test("snappy") {
    fs2(fs2Root / "tiger.json.snappy", TestData.tigerSet)
  }
  test("bzip2") {
    fs2(fs2Root / "tiger.json.bz2", TestData.tigerSet)
  }
  test("lz4") {
    fs2(fs2Root / "tiger.json.lz4", TestData.tigerSet)
  }

  test("deflate") {
    fs2(fs2Root / "tiger.json.deflate", TestData.tigerSet)
  }

  ignore("ZSTANDARD") {
    fs2(fs2Root / "tiger.json.zst", TestData.tigerSet)
  }

  test("laziness") {
    hdp.bytes.source(NJPath("./does/not/exist"))
    hdp.bytes.sink(NJPath("./does/not/exist"))
  }
}
