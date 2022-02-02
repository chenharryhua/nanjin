package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.CirceSerde
import fs2.Stream
import io.circe.generic.auto.*
import org.scalatest.funsuite.AnyFunSuite

class CircePipeTest extends AnyFunSuite {
  import TestData.*
  val data: Stream[IO, Tiger] = Stream.emits(tiggers)

  test("circe identity - remove null") {
    assert(
      data
        .through(CirceSerde.serPipe[IO, Tiger](isKeepNull = false))
        .through(CirceSerde.deserPipe[IO, Tiger])
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
  test("circe identity - keep null") {
    assert(
      data
        .through(CirceSerde.serPipe[IO, Tiger](isKeepNull = true))
        .through(CirceSerde.deserPipe[IO, Tiger])
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
