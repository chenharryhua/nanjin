package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{Compression, ProtobufFile}
import eu.timepit.refined.auto.*
import example.hadoop
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import mtest.pb.test.Lion
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite
import scalapb.GeneratedMessageCompanion

class ProtobufTerminalTest extends AnyFunSuite {
  import ProtobufData.*

  val root: Url                            = Url.parse("./data/example/protobuf")
  val data: Stream[IO, Lion]               = Stream.emits(lions)
  val gmc: GeneratedMessageCompanion[Lion] = implicitly
  def run(file: ProtobufFile): Assertion = {
    val path: Url = root / file.fileName

    val write =
      Stream
        .resource(hadoop.sink(path).outputStream)
        .flatMap(os => data.map(_.writeDelimitedTo(os)))
        .compile
        .drain

    val read = Stream
      .resource(hadoop.source(path).inputStream)
      .flatMap(is => Stream.fromIterator[IO].apply(gmc.streamFromDelimitedInput(is).iterator, 1))
      .compile
      .toList

    val res = (hadoop.delete(path) >> write >> read).unsafeRunSync()
    assert(lions === res)
  }

  test("1.uncompressed") {
    run(ProtobufFile(Compression.Uncompressed))
  }
  test("2.snappy") {
    run(ProtobufFile(Compression.Snappy))
  }
  test("3.bzip2") {
    run(ProtobufFile(Compression.Bzip2))
  }
  test("4.gzip") {
    run(ProtobufFile(Compression.Gzip))
  }
  test("5.lzo") {
    run(ProtobufFile(Compression.Lzo))
  }
  test("6.lz4") {
    run(ProtobufFile(Compression.Lz4))
  }
  test("7.lz4_raw") {
    run(ProtobufFile(Compression.Lz4_Raw))
  }
  test("8.brotli") {
    run(ProtobufFile(Compression.Brotli))
  }
  test("9.deflate-3") {
    run(ProtobufFile(Compression.Deflate(3)))
  }
  test("10.xz") {
    run(ProtobufFile(Compression.Xz(3)))
  }

}
