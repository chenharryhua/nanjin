package mtest.pipes

import cats.effect.IO
import com.github.chenharryhua.nanjin.pipes.GenericRecordCodec
import fs2.Stream
import org.scalatest.funsuite.AnyFunSuite

class GenericRecordPipeTest extends AnyFunSuite {
  import TestData._
  val ser = new GenericRecordCodec[IO, Tigger]

  test("generic-record identity") {
    val data: Stream[IO, Tigger] = Stream.emits(tiggers)

    assert(
      data
        .through(ser.encode(Tigger.avroEncoder))
        .through(ser.decode(Tigger.avroDecoder))
        .compile
        .toList
        .unsafeRunSync() === tiggers)
  }
}
