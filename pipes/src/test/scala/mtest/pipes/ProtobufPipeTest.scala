package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{DelimitedProtoBufSerialization, ProtoBufSerialization}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global

class ProtobufPipeTest extends AnyFunSuite {
  import TestData._

  test("delimited protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)
    val ser                    = new DelimitedProtoBufSerialization[IO]

    assert(data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList.unsafeRunSync() === lions)
  }

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    val ser = new ProtoBufSerialization[IO]

    assert(data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList.unsafeRunSync() === lions)
  }

}
