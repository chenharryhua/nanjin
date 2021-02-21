package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.CirceSerialization
import fs2.Stream
import io.circe.generic.auto._
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers

class CircePipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val ser                      = new CirceSerialization[IO, Tigger]
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  "circe pipe" - {

    "circe identity - remove null" in {
      val run = data.through(ser.serialize(isKeepNull = false)).through(ser.deserialize).compile.toList
      run.asserting(_ shouldBe tiggers)
    }
    "circe identity - keep null" in {
      val run = data.through(ser.serialize(isKeepNull = true)).through(ser.deserialize).compile.toList
      run.asserting(_ shouldBe tiggers)
    }
  }
}
