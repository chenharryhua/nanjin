package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.CirceSerialization
import fs2.Stream
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite

class CircePipeTest extends AnyFunSuite {
  import TestData._
  val ser                      = new CirceSerialization[IO, Tigger]
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("circe identity") {
    assert(
      data
        .through(ser.serialize)
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
