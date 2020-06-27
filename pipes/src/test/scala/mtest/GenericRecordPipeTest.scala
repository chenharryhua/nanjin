package mtest

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.{GenericRecordDecoder, GenericRecordEncoder}
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class GenericRecordPipeTest extends AnyFunSuite {
  import TestData._
  val ser  = new GenericRecordEncoder[IO, Tigger]
  val dser = new GenericRecordDecoder[IO, Tigger]

  test("generic-record identity") {
    val data: Stream[IO, Tigger] = Stream.emits(list)

    assert(data.through(ser.encode).through(dser.decode).compile.toList.unsafeRunSync() === list)
  }
}
