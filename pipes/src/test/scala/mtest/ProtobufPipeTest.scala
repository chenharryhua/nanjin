package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{DelimitedProtoBufSerialization, ProtoBufSerialization}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufPipeTest extends AnyFunSuite {
  import TestData._

  test("delimited protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)
    val ser                    = new DelimitedProtoBufSerialization[IO, Lion]

    assert(
      data
        .through(ser.serialize(blocker))
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === lions)
  }

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    val ser = new ProtoBufSerialization[IO, Lion]

    assert(
      data.through(ser.serialize).through(ser.deserialize).compile.toList.unsafeRunSync() === lions)
  }

}
