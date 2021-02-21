package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.JavaObjectSerialization
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JavaObjectPipeTest extends AsyncFreeSpec with AsyncIOSpec with Matchers {
  import TestData._
  val ser = new JavaObjectSerialization[IO, Tigger]

  "java object" - {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    "identity" in {
      val run = data.through(ser.serialize).through(ser.deserialize).compile.toList
      run.asserting(_ shouldBe tiggers)
    }
  }
}
