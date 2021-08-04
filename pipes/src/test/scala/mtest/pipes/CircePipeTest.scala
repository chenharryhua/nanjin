package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CirceSerialization
import fs2.Stream
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

class CircePipeTest extends AnyFunSuite {
  import TestData.*
  val ser                      = new CirceSerialization[IO, Tigger]
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("circe identity - remove null") {
    assert(
      data
        .through(ser.serialize(isKeepNull = false))
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
  test("circe identity - keep null") {
    assert(
      data
        .through(ser.serialize(isKeepNull = true))
        .through(ser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }

}
