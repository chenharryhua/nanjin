package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.spark.SparkSessionExt
import com.github.chenharryhua.nanjin.terminals.ProtobufFile
import eu.timepit.refined.auto.*
import example.{hadoop, sparkSession}
import fs2.Stream
import io.lemonlabs.uri.Url
import io.lemonlabs.uri.typesafe.dsl.urlToUrlDsl
import mtest.pb.test.Lion
import org.scalatest.Assertion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufTerminalTest extends AnyFunSuite {
  import ProtobufData.*

  val root: Url = Url.parse("./data/example/protobuf")
  val data: Stream[IO, Lion] = Stream.emits(lions)
  def run(file: ProtobufFile): Assertion = {
    val path: Url = root / file.fileName

    val write = data.through(hadoop.sink(path).protobuf).compile.drain

    val read = hadoop.source(path).protobuf[Lion](100).compile.toList

    val res = (hadoop.delete(path) >> write >> read).unsafeRunSync()
    assert(lions === res)
    assert(sparkSession.loadProtobuf[Lion](path).collect().toSet == lions.toSet)
  }

  test("1.uncompressed") {
    run(ProtobufFile(_.Uncompressed))
  }
  test("2.snappy") {
    run(ProtobufFile(_.Snappy))
  }
  test("3.bzip2") {
    run(ProtobufFile(_.Bzip2))
  }
  test("4.gzip") {
    run(ProtobufFile(_.Gzip))
  }
  test("6.lz4") {
    run(ProtobufFile(_.Lz4))
  }
  test("9.deflate-3") {
    run(ProtobufFile(_.Deflate(3)))
  }

}
