package example.protobuf

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.{DelimitedProtoBufSerialization, ProtoBufSerialization}
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

    val ser = new DelimitedProtoBufSerialization[IO]

    assert(
      data
        .through(ser.serialize(Kilobytes(10)))
        .through(ser.deserialize[Lion](ChunkSize(5)))
        .compile
        .toList
        .unsafeRunSync() === lions)
  }

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    val ser = new ProtoBufSerialization[IO]

    assert(data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList.unsafeRunSync() === lions)
  }

}
