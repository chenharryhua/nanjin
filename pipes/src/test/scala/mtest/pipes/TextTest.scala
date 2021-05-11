package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.TextSerialization
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global

class TextTest extends AnyFunSuite {
  import TestData._
  val ser: TextSerialization[IO] = new TextSerialization[IO]
  val expected: List[String]     = tiggers.map(_.toString)
  val data: Stream[IO, String]   = Stream.emits(expected)

  test("text identity") {
    assert(data.through(ser.serialize).through(ser.deserialize).compile.toList.unsafeRunSync() === expected)
  }
}
