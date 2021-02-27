package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.JavaObjectSerialization
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import cats.effect.unsafe.implicits.global

class JavaObjectPipeTest extends AnyFunSuite {
  import TestData._
  val ser = new JavaObjectSerialization[IO, Tigger]

  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("identity") {
    val run = data.through(ser.serialize).through(ser.deserialize).compile.toList
    assert(run.unsafeRunSync() == tiggers)

  }
}
