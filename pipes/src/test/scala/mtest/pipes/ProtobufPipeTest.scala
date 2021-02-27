package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.{DelimitedProtoBufSerialization, ProtoBufSerialization}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite

class ProtobufPipeTest extends AnyFunSuite {
  import TestData._
  val data: Stream[IO, Lion] = Stream.emits(lions)

  test("delimited protobuf identity") {
    val ser = new DelimitedProtoBufSerialization[IO]
    val run = data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList
    assert(run.unsafeRunSync() == lions)
  }

  test("protobuf identity") {
    val ser = new ProtoBufSerialization[IO]
    val run =
      data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList
    assert(run.unsafeRunSync() == lions)
  }
}
