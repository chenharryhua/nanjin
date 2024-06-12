package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.javaObject
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
class JavaObjectPipeTest extends AnyFunSuite {
  import mtest.terminals.TestData.*
  test("java object identity") {
    val data: Stream[IO, Tiger] = Stream.emits(tigers)

    assert(
      data
        .through(javaObject.toBytes)
        .through(javaObject.fromBytes)
        .compile
        .toList
        .unsafeRunSync() === tigers)
  }
}
