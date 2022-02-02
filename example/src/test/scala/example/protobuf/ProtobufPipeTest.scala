package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.{DelimitedProtoBufSerde, ProtoBufSerde}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
import squants.information.Kilobytes

import scala.util.Random
import com.github.chenharryhua.nanjin.common.ChunkSize

class ProtobufPipeTest extends AnyFunSuite {
  val lions: List[Lion] =
    (1 to 10).map(x => Lion("Melbourne Zoo", Random.nextInt())).toList

  test("delimited protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    assert(
      data
        .through(DelimitedProtoBufSerde.serPipe[IO, Lion](Kilobytes(10)))
        .through(DelimitedProtoBufSerde.deserPipe[IO, Lion](ChunkSize(5)))
        .compile
        .toList
        .unsafeRunSync() === lions)
  }

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    assert(
      data
        .through(ProtoBufSerde.serPipe[IO, Lion])
        .through(ProtoBufSerde.deserPipe[IO, Lion])
        .compile
        .toList
        .unsafeRunSync() === lions)
  }

}
