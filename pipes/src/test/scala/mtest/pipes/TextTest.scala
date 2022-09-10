package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.TextSerde
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class TextTest extends AnyFunSuite {
  import TestData.*
  val expected: List[String] = tigers.map(_.toString)

  test("fs2 text identity") {
    val data: Stream[IO, String] = Stream.emits(expected)
    assert(
      data
        .through(TextSerde.toBytes)
        .through(TextSerde.fromBytes)
        .compile
        .toList
        .unsafeRunSync() === expected)
  }

}
