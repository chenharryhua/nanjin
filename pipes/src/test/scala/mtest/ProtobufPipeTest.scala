package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{
  DelimitedProtoBufDeserialization,
  DelimitedProtoBufSerialization,
  ProtoBufDeserialization,
  ProtoBufSerialization
}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufPipeTest extends AnyFunSuite {
  import TestData._

  test("delimited protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)
    val ser                    = new DelimitedProtoBufSerialization[IO, Lion](blocker)
    val dser                   = new DelimitedProtoBufDeserialization[IO, Lion]

    assert(
      data
        .through(ser.serialize)
        .through(dser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === lions)
  }

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    val ser  = new ProtoBufSerialization[IO, Lion]
    val dser = new ProtoBufDeserialization[IO, Lion]

    assert(
      data
        .through(ser.serialize)
        .through(dser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === lions)
  }

}
