package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.datetime.zones.sydneyTime
import com.github.chenharryhua.nanjin.datetime.{crontabs, policies}
import com.github.chenharryhua.nanjin.terminals.{HadoopProtobuf, NJCompression, NJPath, ProtobufFile}
import eu.timepit.refined.auto.*
import example.hadoop
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufTerminalTest extends AnyFunSuite {
  import ProtobufData.*

  val root: NJPath           = NJPath("./data/example/protobuf")
  val data: Stream[IO, Lion] = Stream.emits(lions)
  val pb: HadoopProtobuf[IO] = hadoop.protobuf

  def run(file: ProtobufFile): Assertion = {
    val path: NJPath = root / file.fileName
    val res = (hadoop.delete(path) >> data.through(pb.sink[Lion](path)).compile.drain >>
      pb.source[Lion](path).compile.toList).unsafeRunSync()
    assert(lions === res)
  }

  test("1.uncompressed") {
    run(ProtobufFile(NJCompression.Uncompressed))
  }
  test("2.snappy") {
    run(ProtobufFile(NJCompression.Snappy))
  }
  test("3.bzip2") {
    run(ProtobufFile(NJCompression.Bzip2))
  }
  test("4.gzip") {
    run(ProtobufFile(NJCompression.Gzip))
  }
  test("5.lzo") {
    run(ProtobufFile(NJCompression.Lzo))
  }
  test("6.lz4") {
    run(ProtobufFile(NJCompression.Lz4))
  }
  test("7.lz4_raw") {
    run(ProtobufFile(NJCompression.Lz4_Raw))
  }
  test("8.brotli") {
    run(ProtobufFile(NJCompression.Brotli))
  }
  test("9.deflate-3") {
    run(ProtobufFile(NJCompression.Deflate(3)))
  }
  test("10.xz") {
    run(ProtobufFile(NJCompression.Xz(3)))
  }
  test("rotate") {
    val file  = ProtobufFile(NJCompression.Uncompressed)
    val path  = root / "rotate"
    val sink  = pb.sink(policies.cronBackoff[IO](crontabs.secondly, sydneyTime))(t => path / file.fileName(t))
    val write = Stream.emits(herd).chunkN(1).unchunks.through(sink).compile.drain
    val read  = Stream.eval(hadoop.filesIn(path)).flatMap(pb.source[Lion]).compile.toList
    val res   = hadoop.delete(path) >> write >> read
    assert(res.unsafeRunSync().size === 10000)
  }
}
