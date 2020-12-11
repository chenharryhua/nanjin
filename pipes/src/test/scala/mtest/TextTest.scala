package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.TextSerialization
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class TextTest extends AnyFunSuite {
  import TestData._
  val ser                      = new TextSerialization[IO]
  val data: Stream[IO, String] = Stream.emits(tiggers).map(_.toString)

  test("text identity") {
    assert(
      data
        .through(ser.serialize)
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
