package mtest

import cats.effect.IO
import cats.implicits._
import com.github.chenharryhua.nanjin.pipes.{CirceDeserialization, CirceSerialization}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import io.circe.generic.auto._
import io.circe.Encoder
import io.circe.Decoder

class CircePipeTest extends AnyFunSuite {
  import TestData._
  val ser                      = new CirceSerialization[IO, Tigger](Encoder[Tigger])
  val dser                     = new CirceDeserialization[IO, Tigger](Decoder[Tigger])
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("circe identity") {
    assert(
      data
        .through(ser.serialize)
        .through(dser.deserialize)
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
