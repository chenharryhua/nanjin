package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.JavaObjectSerialization
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class JavaObjectPipeTest extends AnyFunSuite {
  import TestData._
  val ser = new JavaObjectSerialization[IO, Tigger]

  test("java object identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(
      data
        .through(ser.serialize)
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
