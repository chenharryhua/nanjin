package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.GenericRecordCodec
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class GenericRecordPipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val ser = new GenericRecordCodec[IO, Tigger]

  "generic record" - {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    "identity" in {
      val run = data.through(ser.encode(Tigger.avroEncoder)).through(ser.decode(Tigger.avroDecoder)).compile.toList
      run.asserting(_ shouldBe tiggers)
    }
  }
}
