package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{
  GenericRecordDeserialization,
  GenericRecordSerialization
}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class GenericRecordPipeTest extends AnyFunSuite {
  import TestData._
  val ser                    = new GenericRecordSerialization[IO, Test]
  val dser                   = new GenericRecordDeserialization[IO, Test]
  val data: Stream[IO, Test] = Stream.fromIterator[IO](list.iterator)

  test("generic-record identity") {
    assert(
      data.through(ser.serialize).through(dser.deserialize).compile.toList.unsafeRunSync() === list)
  }
}
