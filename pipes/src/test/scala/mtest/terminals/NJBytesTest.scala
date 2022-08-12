package mtest.terminals

import akka.stream.scaladsl.Source
import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.CirceSerde
import com.github.chenharryhua.nanjin.terminals.NJPath
import mtest.terminals.HadoopTestData.hdp
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
import cats.effect.unsafe.implicits.global
import fs2.Stream
import mtest.pipes.TestData.Tiger
import io.circe.generic.auto.*
import mtest.pipes.TestData
import org.apache.hadoop.io.compress.zlib.ZlibCompressor
import org.scalatest.Assertion

class NJBytesTest extends AnyFunSuite {
  def akka(path: NJPath, data: Set[Tiger]): Assertion = {
    hdp.delete(path).unsafeRunSync()
    val ts   = Source(data)
    val sink = hdp.bytes.akka.sink(path)
    val src  = hdp.bytes.akka.source(path)
    val action = IO.fromFuture(IO(ts.via(CirceSerde.akka.toByteString(true)).runWith(sink))) >>
      IO.fromFuture(
        IO(src.via(CirceSerde.akka.fromByteString[Tiger]).runFold(Set.empty[Tiger]) { case (ss, i) =>
          ss + i
        }))
    assert(action.unsafeRunSync() == data)
  }

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
    akka(akkaRoot / "tiger.json", TestData.tigerSet)
    fs2(fs2Root / "tiger.json", TestData.tigerSet)
  }

  test("gzip") {
    akka(akkaRoot / "tiger.json.gz", TestData.tigerSet)
    fs2(fs2Root / "tiger.json.gz", TestData.tigerSet)
  }
  test("snappy") {
    akka(akkaRoot / "tiger.json.snappy", TestData.tigerSet)
    fs2(fs2Root / "tiger.json.snappy", TestData.tigerSet)
  }
  test("bzip2") {
    akka(akkaRoot / "tiger.json.bz2", TestData.tigerSet)
    fs2(fs2Root / "tiger.json.bz2", TestData.tigerSet)
  }
  test("lz4") {
    akka(akkaRoot / "tiger.json.lz4", TestData.tigerSet)
    fs2(fs2Root / "tiger.json.lz4", TestData.tigerSet)
  }

  test("deflate") {
    fs2(fs2Root / "tiger.json.deflate", TestData.tigerSet)
    akka(akkaRoot / "tiger.json.deflate", TestData.tigerSet)
  }

  ignore("ZSTANDARD") {
    akka(akkaRoot / "tiger.json.zst", TestData.tigerSet)
    fs2(fs2Root / "tiger.json.zst", TestData.tigerSet)
  }
}
