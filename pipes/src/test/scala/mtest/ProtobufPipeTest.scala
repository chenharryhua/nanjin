package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{ProtoBufDeserialization, ProtoBufSerialization}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufPipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new ProtoBufSerialization[IO, Lion](blocker)
  val dser = new ProtoBufDeserialization[IO, Lion]

  test("identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)
    assert(
      data
        .through(ser.serialize)
        .through(dser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === lions)
  }
}
