package mtest.pipes

import cats.effect.IO
import cats.effect.testing.scalatest.AsyncIOSpec
import com.github.chenharryhua.nanjin.pipes.GenericRecordCodec
import fs2.Stream
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import cats.effect.unsafe.implicits.global
import org.scalatest.funsuite.AnyFunSuite

class GenericRecordPipeTest extends AnyFunSuite {
  import TestData._
  val ser = new GenericRecordCodec[IO, Tigger]

  val data: Stream[IO, Tigger] = Stream.emits(tiggers)

  test("identity") {
    val run = data.through(ser.encode(Tigger.avroEncoder)).through(ser.decode(Tigger.avroDecoder)).compile.toList
    assert(run.unsafeRunSync() == tiggers)
  }

}
