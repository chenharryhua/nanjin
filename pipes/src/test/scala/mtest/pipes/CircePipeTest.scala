package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.CirceSerialization
import fs2.Stream
import io.circe.generic.auto._
import org.scalatest.funsuite.AnyFunSuite
import cats.effect.unsafe.implicits.global

class CircePipeTest extends AnyFunSuite {
  import TestData._
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
