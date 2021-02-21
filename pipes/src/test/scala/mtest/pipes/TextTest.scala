package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.TextSerialization
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class TextTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val ser: TextSerialization[IO] = new TextSerialization[IO]
  val expected: List[String]     = tiggers.map(_.toString)
  val data: Stream[IO, String]   = Stream.emits(expected)

  "text" - {
    "text identity" in {
      val run = data.through(ser.serialize).through(ser.deserialize).compile.toList
      run.asserting(_ shouldBe expected)
    }
  }
}
