package mtest.pipes

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import com.github.chenharryhua.nanjin.pipes.serde.JavaObjectSerialization
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite
import eu.timepit.refined.auto.*
class JavaObjectPipeTest extends AnyFunSuite {
  import TestData.*
  val ser = new JavaObjectSerialization[IO, Tigger]

  test("java object identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(data.through(ser.serialize(100)).through(ser.deserialize).compile.toList.unsafeRunSync() === tiggers)
  }
}
