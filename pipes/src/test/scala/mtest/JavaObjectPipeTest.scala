package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{JavaObjectDeserialization, JavaObjectSerialization}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class JavaObjectPipeTest extends AnyFunSuite {
  import TestData._
  val ser                    = new JavaObjectSerialization[IO, Test]
  val dser                   = new JavaObjectDeserialization[IO, Test]
  val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)

  test("java object identity") {
    assert(
      data.through(ser.serialize).through(dser.deserialize).compile.toList.unsafeRunSync() === list)
  }
}
