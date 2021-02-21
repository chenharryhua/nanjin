package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.{BinaryAvroSerialization, GenericRecordCodec}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class BinaryAvroPipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val gr                       = new GenericRecordCodec[IO, Tigger]
  val ba                       = new BinaryAvroSerialization[IO](AvroSchema[Tigger])
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  "binary-json pipe" - {

    "identity" in {
      val run = data
        .through(gr.encode(Tigger.avroEncoder))
        .through(ba.serialize)
        .through(ba.deserialize)
        .through(gr.decode(Tigger.avroDecoder))
        .compile
        .toList
      run.asserting(_ shouldBe tiggers)
    }
  }
}
