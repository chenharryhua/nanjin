package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.TextSerialization
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class TextTest extends AnyFunSuite {
  import TestData.*
  val ser: TextSerialization[IO] = new TextSerialization[IO]
  val expected: List[String]     = tiggers.map(_.toString)
  val data: Stream[IO, String]   = Stream.emits(expected)

  test("text identity") {
    assert(data.through(ser.serialize).through(ser.deserialize).compile.toList.unsafeRunSync() === expected)
  }
}
