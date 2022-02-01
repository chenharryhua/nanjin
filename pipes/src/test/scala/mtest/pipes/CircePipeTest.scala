package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CirceSerde
import fs2.Stream
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

class CircePipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("circe identity - remove null") {
    assert(
      data
        .through(CirceSerde.serialize[IO, Tigger](isKeepNull = false))
        .through(CirceSerde.deserialize[IO, Tigger])
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
  test("circe identity - keep null") {
    assert(
      data
        .through(CirceSerde.serialize[IO, Tigger](isKeepNull = true))
        .through(CirceSerde.deserialize[IO, Tigger])
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
