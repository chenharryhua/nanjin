package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.TextSerde
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class TextTest extends AnyFunSuite {
  import TestData.*
  val expected: List[String]   = tiggers.map(_.toString)
  val data: Stream[IO, String] = Stream.emits(expected)

  test("text identity") {
    assert(data.through(TextSerde.serPipe).through(TextSerde.deserPipe).compile.toList.unsafeRunSync() === expected)
  }
}
