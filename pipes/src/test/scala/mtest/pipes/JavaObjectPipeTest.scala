package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.JavaObjectSerde
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
class JavaObjectPipeTest extends AnyFunSuite {
  import TestData.*
  test("java object identity") {
    val data: Stream[IO, Tiger] = Stream.emits(tigers)

    assert(
      data
        .through(JavaObjectSerde.toBytes)
        .through(JavaObjectSerde.fromBytes)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }
}
