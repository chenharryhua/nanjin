package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.terminals.{HadoopProtobuf, NJPath}
import eu.timepit.refined.auto.*
import example.hadoop
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufPipeTest extends AnyFunSuite {
  import ProtobufData.*

  val root: NJPath = NJPath("./data/example/protobuf")

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)
    val path: NJPath           = root / "lions.pb"
    val pb: HadoopProtobuf[IO] = hadoop.protobuf

    val res = data.through(pb.sink[Lion](path)).compile.drain >>
      pb.source[Lion](path).compile.toList

    assert(lions === res.unsafeRunSync())
  }
}
