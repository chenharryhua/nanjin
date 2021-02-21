package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.{GenericRecordCodec, JacksonSerialization}
import com.sksamuel.avro4s.AvroSchema
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class JsonAvroPipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val gser = new GenericRecordCodec[IO, Tigger]
  val ser  = new JacksonSerialization[IO](AvroSchema[Tigger])
  "Avro Json" - {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)
    "json-avro identity" in {
      val run = data
        .through(gser.encode(Tigger.avroEncoder))
        .through(ser.serialize)
        .through(ser.deserialize)
        .through(gser.decode(Tigger.avroDecoder))
        .compile
        .toList
      run.asserting(_ shouldBe tiggers)
    }

    "jackson-compact-string size" in {
      val run = data.through(gser.encode(Tigger.avroEncoder)).through(ser.compactJson).compile.toList

      run.asserting(_.size shouldBe 10)
    }

    "jackson-pretty-string size" in {
      val run = data.through(gser.encode(Tigger.avroEncoder)).through(ser.prettyJson).compile.toList
      run.asserting(_.size shouldBe 10)
    }
  }
}
