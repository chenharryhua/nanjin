package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.{DelimitedProtoBufSerialization, ProtoBufSerialization}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.funsuite.AnyFunSuite

class ProtobufPipeTest extends AnyFunSuite {
  import TestData.*

  test("delimited protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)
    val ser                    = new DelimitedProtoBufSerialization[IO](10)

    assert(data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList.unsafeRunSync() === lions)
  }

  test("protobuf identity") {
    val data: Stream[IO, Lion] = Stream.emits(lions)

    val ser = new ProtoBufSerialization[IO]

    assert(data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList.unsafeRunSync() === lions)
  }

}
