package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.{DelimitedProtoBufSerialization, ProtoBufSerialization}
import fs2.Stream
import mtest.pb.test.Lion
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class ProtobufPipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val data: Stream[IO, Lion] = Stream.emits(lions)
  "Protobuf" - {

    "delimited protobuf identity" in {
      val ser = new DelimitedProtoBufSerialization[IO]
      val run = data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList
      run.asserting(_ shouldBe lions)
    }

    "protobuf identity" in {
      val ser = new ProtoBufSerialization[IO]
      val run =
        data.through(ser.serialize).through(ser.deserialize[Lion]).compile.toList
      run.asserting(_ shouldBe lions)
    }
  }
}
